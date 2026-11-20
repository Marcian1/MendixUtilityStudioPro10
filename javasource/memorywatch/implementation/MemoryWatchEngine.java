package memorywatch.implementation;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.sun.management.GarbageCollectionNotificationInfo;

/**
 * Notices when the heap is still high after a garbage collection and writes down what was going on.
 *
 * Nothing here runs on a request thread and nothing here touches the database on the notification thread:
 * the JVM notifies us, we copy a few numbers into a snapshot and hand it to a writer thread. Every entry point
 * catches Throwable, so a bug here can cost a diagnostic row but never a request.
 *
 * Three sources feed a snapshot: the old-generation pool (the JVM tells us when it is still above the threshold
 * after a collection), FlowScope's list of running microflows (read by reflection, so FlowScope is optional) and
 * a sampler that reads how much each thread allocated in the last minute.
 */
public final class MemoryWatchEngine {
    private static final ILogNode LOG = Core.getLogger("MemoryWatch");
    private static final MemoryWatchEngine INSTANCE = new MemoryWatchEngine();
    private static final long MB = 1024L * 1024L;
    private static final long SAMPLE_SECONDS = 10;
    private static final int WINDOWS = 6;
    private static final int TOP_THREADS = 5;
    private static final long MIN_THREAD_MB = 1;
    private static final long FLOOR_BUCKET_MS = 60_000L;

    private final EventWriter writer = new EventWriter();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "MemoryWatch-sampler");
        t.setDaemon(true);
        return t;
    });
    private final com.sun.management.ThreadMXBean threads = threadBean();

    private final AtomicLong recorded = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    private volatile boolean enabled;
    private volatile boolean sampleThreads = true;
    private volatile long minIntervalMs = 60_000L;
    private volatile long lastEventMs;
    private volatile int thresholdPercent = 80;
    private volatile String poolName;
    private volatile long poolMaxBytes;
    private volatile long thresholdBytes;
    private boolean listenersAdded;
    private boolean samplerStarted;
    private long lastSampleMs;
    private boolean baselineTaken;

    // thread allocation sampling: last absolute reading per thread id, and a ring of per-window deltas
    private final Map<Long, Long> lastBytes = new HashMap<>();
    private final Map<Long, String> names = new HashMap<>();
    private final ArrayDeque<Map<Long, Long>> windows = new ArrayDeque<>();

    // memory floor: the lowest old-generation level seen after a GC in the current minute
    private final Object floorLock = new Object();
    private long floorBucket;
    private long floorMinUsed = Long.MAX_VALUE;
    private long floorMax;

    private MemoryWatchEngine() {
    }

    public static MemoryWatchEngine get() {
        return INSTANCE;
    }

    // ---- configuration -----------------------------------------------------------------------

    /** Applies the settings. Safe to call again whenever they change, and every minute from a scheduled event. */
    public synchronized void configure(boolean enable, int percent, int minIntervalSeconds, boolean sample) {
        try {
            this.thresholdPercent = Math.max(1, Math.min(99, percent));
            this.minIntervalMs = Math.max(1, minIntervalSeconds) * 1000L;
            this.sampleThreads = sample && threads != null && threads.isThreadAllocatedMemorySupported();
            MemoryPoolMXBean pool = findOldPool();
            if (pool == null) {
                LOG.warn("MemoryWatch found no old-generation memory pool that supports a collection threshold, so it cannot watch heap pressure on this JVM.");
                this.enabled = false;
                return;
            }
            this.poolName = pool.getName();
            long max = pool.getUsage().getMax();
            this.poolMaxBytes = max > 0 ? max : Runtime.getRuntime().maxMemory();
            if (enable) {
                this.thresholdBytes = poolMaxBytes / 100 * thresholdPercent;
                pool.setCollectionUsageThreshold(thresholdBytes);
                writer.ensureStarted();
                addListeners();
                startSampler();
                if (this.sampleThreads && lastBytes.isEmpty()) {
                    sampleAllocation();
                }
                if (!this.enabled) {
                    LOG.info("MemoryWatch is on: pool '" + poolName + "', max " + poolMaxBytes / MB + " MB, event above "
                            + thresholdPercent + "% after a collection (" + thresholdBytes / MB + " MB).");
                }
                this.enabled = true;
            } else {
                if (this.enabled) {
                    LOG.info("MemoryWatch is off.");
                }
                this.enabled = false;
                this.thresholdBytes = 0;
                pool.setCollectionUsageThreshold(0);
            }
        } catch (Throwable t) {
            this.enabled = false;
            LOG.warn("MemoryWatch could not apply its settings: " + t, t);
        }
    }

    private static com.sun.management.ThreadMXBean threadBean() {
        try {
            java.lang.management.ThreadMXBean b = ManagementFactory.getThreadMXBean();
            return b instanceof com.sun.management.ThreadMXBean ? (com.sun.management.ThreadMXBean) b : null;
        } catch (Throwable t) {
            return null;
        }
    }

    /** The old generation ("G1 Old Gen", "PS Old Gen", "Tenured Gen"), or failing that the biggest heap pool that can be watched. */
    private static MemoryPoolMXBean findOldPool() {
        MemoryPoolMXBean best = null;
        for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
            if (p.getType() != MemoryType.HEAP || !p.isCollectionUsageThresholdSupported()) {
                continue;
            }
            String n = p.getName();
            if (n.contains("Old") || n.contains("Tenured")) {
                return p;
            }
            if (best == null || p.getUsage().getMax() > best.getUsage().getMax()) {
                best = p;
            }
        }
        return best;
    }

    private void addListeners() {
        if (listenersAdded) {
            return;
        }
        listenersAdded = true;
        NotificationListener thresholdListener = (Notification n, Object hb) -> {
            try {
                if (MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED.equals(n.getType())) {
                    MemoryNotificationInfo info = MemoryNotificationInfo.from((CompositeData) n.getUserData());
                    if (info.getPoolName().equals(poolName)) {
                        onPressure(info.getUsage());
                    }
                }
            } catch (Throwable t) {
                // Never let a diagnostic hurt the JVM's notification thread.
            }
        };
        ((NotificationEmitter) ManagementFactory.getMemoryMXBean()).addNotificationListener(thresholdListener, null, null);

        NotificationListener gcListener = (Notification n, Object hb) -> {
            try {
                if (enabled && GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(n.getType())) {
                    GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) n.getUserData());
                    MemoryUsage after = info.getGcInfo().getMemoryUsageAfterGc().get(poolName);
                    if (after != null) {
                        onFloor(after.getUsed(), after.getMax() > 0 ? after.getMax() : poolMaxBytes);
                    }
                }
            } catch (Throwable t) {
                // Same rule as above.
            }
        };
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc instanceof NotificationEmitter) {
                ((NotificationEmitter) gc).addNotificationListener(gcListener, null, null);
            }
        }
    }

    // ---- pressure events ---------------------------------------------------------------------

    private void onPressure(MemoryUsage afterGc) {
        if (!enabled) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastEventMs < minIntervalMs) {
            skipped.incrementAndGet();
            return;
        }
        lastEventMs = now;
        offer(snapshot(now, afterGc.getUsed(), afterGc.getMax() > 0 ? afterGc.getMax() : poolMaxBytes, null));
    }

    /** Records an event on demand, whatever the heap level, for trying the page out. Not debounced. */
    public void recordTestEvent() {
        try {
            MemoryPoolMXBean pool = findOldPool();
            MemoryUsage cu = pool == null ? null : pool.getCollectionUsage();
            long used = cu != null && cu.getUsed() >= 0 ? cu.getUsed() : (pool == null ? 0 : pool.getUsage().getUsed());
            long max = poolMaxBytes > 0 ? poolMaxBytes : Runtime.getRuntime().maxMemory();
            writer.ensureStarted();
            offer(snapshot(System.currentTimeMillis(), used, max, "Recorded by hand, not caused by memory pressure."));
        } catch (Throwable t) {
            LOG.warn("MemoryWatch could not record a test event: " + t, t);
        }
    }

    private Snapshot snapshot(long now, long oldUsed, long oldMax, String note) {
        Snapshot s = new Snapshot();
        s.timeMs = now;
        s.oldUsedMb = oldUsed / MB;
        s.oldMaxMb = oldMax / MB;
        s.percent = oldMax > 0 ? (int) Math.min(100, oldUsed * 100 / oldMax) : 0;
        Runtime rt = Runtime.getRuntime();
        s.heapUsedMb = (rt.totalMemory() - rt.freeMemory()) / MB;
        s.heapMaxMb = rt.maxMemory() / MB;
        s.note = note;
        List<Object[]> running = runningFlows();
        s.flowsKnown = running != null;
        if (running != null) {
            for (Object[] r : running) {
                if (isOwnPlumbing((String) r[0])) {
                    continue;
                }
                s.flows.add(new Snapshot.Flow((String) r[0], ((Long) r[1]) / 1000.0, r.length < 3 || r[2] == null));
            }
        }
        s.threads.addAll(busiestThreads());
        return s;
    }

    private void offer(Snapshot s) {
        if (writer.offer(s)) {
            recorded.incrementAndGet();
        } else {
            dropped.incrementAndGet();
        }
    }

    /** MemoryWatch's own page flows (status tiles, buttons, scheduled events) are not suspects; its playground flows are. */
    private static boolean isOwnPlumbing(String flow) {
        return flow != null && (flow.startsWith("MemoryWatch.DS_") || flow.startsWith("MemoryWatch.ACT_") || flow.startsWith("MemoryWatch.SE_"));
    }

    /** FlowScope's running microflows, or null when FlowScope is absent or its capture is off. */
    @SuppressWarnings("unchecked")
    private static List<Object[]> runningFlows() {
        try {
            Class<?> c = Class.forName("flowscope.implementation.CaptureEngine");
            Object engine = c.getMethod("get").invoke(null);
            return (List<Object[]>) c.getMethod("runningFlows").invoke(engine);
        } catch (Throwable t) {
            return null;
        }
    }

    // ---- memory floor ------------------------------------------------------------------------

    private void onFloor(long used, long max) {
        long now = System.currentTimeMillis();
        long bucket = now / FLOOR_BUCKET_MS * FLOOR_BUCKET_MS;
        synchronized (floorLock) {
            flushFloorLocked(bucket);
            floorBucket = bucket;
            floorMinUsed = Math.min(floorMinUsed, used);
            floorMax = max;
        }
    }

    /** Hands over the finished minute, if there is one. */
    private void flushFloorLocked(long currentBucket) {
        if (floorBucket != 0 && floorBucket < currentBucket && floorMinUsed != Long.MAX_VALUE) {
            writer.offerFloor(new Snapshot.Floor(floorBucket, floorMinUsed / MB, floorMax / MB));
            floorBucket = 0;
            floorMinUsed = Long.MAX_VALUE;
        }
    }

    // ---- thread allocation sampling ----------------------------------------------------------

    private void startSampler() {
        if (samplerStarted) {
            return;
        }
        samplerStarted = true;
        scheduler.scheduleWithFixedDelay(this::tick, SAMPLE_SECONDS, SAMPLE_SECONDS, TimeUnit.SECONDS);
    }

    private void tick() {
        try {
            if (!enabled) {
                return;
            }
            synchronized (floorLock) {
                flushFloorLocked(System.currentTimeMillis() / FLOOR_BUCKET_MS * FLOOR_BUCKET_MS);
            }
            if (sampleThreads) {
                sampleAllocation();
            }
        } catch (Throwable t) {
            LOG.trace("MemoryWatch sampler tick failed: " + t);
        }
    }

    /** Reads every thread's total allocation and keeps the change since the last reading. Cheap: no stack traces. */
    private synchronized void sampleAllocation() {
        long[] ids = threads.getAllThreadIds();
        long[] bytes = threads.getThreadAllocatedBytes(ids);
        Map<Long, Long> window = new HashMap<>();
        List<Long> unnamed = new ArrayList<>();
        Set<Long> alive = new HashSet<>();
        for (int i = 0; i < ids.length; i++) {
            long id = ids[i];
            if (bytes[i] < 0) {
                continue;
            }
            alive.add(id);
            Long previous = lastBytes.put(id, bytes[i]);
            // A thread born since the last reading allocated everything it has from zero.
            long base = previous != null ? previous : (baselineTaken ? 0 : bytes[i]);
            if (bytes[i] > base) {
                window.put(id, bytes[i] - base);
            }
            if (!names.containsKey(id)) {
                unnamed.add(id);
            }
        }
        if (!unnamed.isEmpty()) {
            long[] want = new long[unnamed.size()];
            for (int i = 0; i < want.length; i++) {
                want[i] = unnamed.get(i);
            }
            java.lang.management.ThreadInfo[] infos = threads.getThreadInfo(want, 0);
            for (int i = 0; i < want.length; i++) {
                names.put(want[i], infos[i] == null ? "thread-" + want[i] : infos[i].getThreadName());
            }
        }
        lastBytes.keySet().retainAll(alive);
        names.keySet().retainAll(alive);
        lastSampleMs = System.currentTimeMillis();
        baselineTaken = true;
        windows.addLast(window);
        while (windows.size() > WINDOWS) {
            windows.removeFirst();
        }
    }

    /** Adds what each thread allocated since the last tick to the totals, without moving the sampling baseline. */
    private void peekAllocation(Map<Long, Long> total) {
        try {
            long[] ids = threads.getAllThreadIds();
            long[] bytes = threads.getThreadAllocatedBytes(ids);
            List<Long> unnamed = new ArrayList<>();
            for (int i = 0; i < ids.length; i++) {
                Long previous = lastBytes.get(ids[i]);
                long base = previous != null ? previous : (baselineTaken ? 0 : bytes[i]);
                if (bytes[i] > base) {
                    total.merge(ids[i], bytes[i] - base, Long::sum);
                    if (!names.containsKey(ids[i])) {
                        unnamed.add(ids[i]);
                    }
                }
            }
            if (!unnamed.isEmpty()) {
                long[] want = new long[unnamed.size()];
                for (int i = 0; i < want.length; i++) {
                    want[i] = unnamed.get(i);
                }
                java.lang.management.ThreadInfo[] infos = threads.getThreadInfo(want, 0);
                for (int i = 0; i < want.length; i++) {
                    names.put(want[i], infos[i] == null ? "thread-" + want[i] : infos[i].getThreadName());
                }
            }
        } catch (Throwable t) {
            // The windows already collected are still reported.
        }
    }

    private synchronized List<Snapshot.ThreadUse> busiestThreads() {
        Map<Long, Long> total = new HashMap<>();
        for (Map<Long, Long> w : windows) {
            for (Map.Entry<Long, Long> e : w.entrySet()) {
                total.merge(e.getKey(), e.getValue(), Long::sum);
            }
        }
        // The allocation since the last tick is not in a window yet, and it is often the part that matters: add it.
        peekAllocation(total);
        int seconds = (int) (windows.size() * SAMPLE_SECONDS + (lastSampleMs == 0 ? 0 : (System.currentTimeMillis() - lastSampleMs) / 1000));
        List<Snapshot.ThreadUse> out = new ArrayList<>();
        total.entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<Long, Long> e) -> e.getValue()).reversed())
                .limit(TOP_THREADS)
                .forEach(e -> {
                    long mb = e.getValue() / MB;
                    if (mb >= MIN_THREAD_MB) {
                        String name = names.getOrDefault(e.getKey(), "thread-" + e.getKey());
                        out.add(new Snapshot.ThreadUse(name, kindOf(name), mb, seconds));
                    }
                });
        return out;
    }

    static String kindOf(String name) {
        if (name.startsWith("JettyServer") || name.startsWith("qtp")) {
            return "Request";
        }
        if (name.startsWith("ActionManager")) {
            return "Scheduled event";
        }
        if (name.startsWith("TaskQueue")) {
            return "Task queue";
        }
        return "Other";
    }

    // ---- status ------------------------------------------------------------------------------

    /** What the engine is doing, for the status tiles on the overview page. */
    public Map<String, Object> status() {
        Map<String, Object> m = new HashMap<>();
        MemoryPoolMXBean pool = findOldPool();
        m.put("Active", enabled);
        m.put("PoolName", pool == null ? "none found" : pool.getName());
        long max = poolMaxBytes > 0 ? poolMaxBytes : (pool == null ? 0 : Math.max(0, pool.getUsage().getMax()));
        m.put("PoolMaxMb", max / MB);
        m.put("ThresholdMb", thresholdBytes / MB);
        long now = pool == null ? 0 : pool.getUsage().getUsed();
        m.put("OldGenUsedMb", now / MB);
        m.put("EventsRecorded", recorded.get());
        m.put("EventsSkipped", skipped.get());
        m.put("EventsDropped", dropped.get());
        m.put("SamplerOn", enabled && sampleThreads);
        m.put("FlowScopeLinked", runningFlows() != null);
        return m;
    }
}
