package memorywatch.implementation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

/** Saves snapshots on its own thread, so the notification that produced them is never held up by the database. */
final class EventWriter {
    private static final ILogNode LOG = Core.getLogger("MemoryWatch");
    private static final int CAPACITY = 200;

    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>(CAPACITY);
    private Thread thread;

    synchronized void ensureStarted() {
        if (thread != null && thread.isAlive()) {
            return;
        }
        thread = new Thread(this::loop, "MemoryWatch-writer");
        thread.setDaemon(true);
        thread.start();
    }

    boolean offer(Snapshot s) {
        return queue.offer(s);
    }

    boolean offerFloor(Snapshot.Floor f) {
        return queue.offer(f);
    }

    private void loop() {
        while (true) {
            try {
                Object o = queue.take();
                if (o instanceof Snapshot) {
                    persist((Snapshot) o);
                } else if (o instanceof Snapshot.Floor) {
                    persistFloor((Snapshot.Floor) o);
                }
            } catch (InterruptedException e) {
                return;
            } catch (Throwable t) {
                LOG.warn("MemoryWatch could not save a record: " + t, t);
            }
        }
    }

    private static void persist(Snapshot s) throws Throwable {
        IContext ctx = Core.createSystemContext();
        ctx.startTransaction();
        try {
            IMendixObject ev = Core.instantiate(ctx, "MemoryWatch.MemoryEvent");
            ev.setValue(ctx, "OccurredAt", new Date(s.timeMs));
            ev.setValue(ctx, "OldGenUsedMB", s.oldUsedMb);
            ev.setValue(ctx, "OldGenMaxMB", s.oldMaxMb);
            ev.setValue(ctx, "UsedPercent", s.percent);
            ev.setValue(ctx, "HeapUsedMB", s.heapUsedMb);
            ev.setValue(ctx, "HeapMaxMB", s.heapMaxMb);
            ev.setValue(ctx, "FlowsKnown", s.flowsKnown);
            ev.setValue(ctx, "RunningFlowCount", s.flows.size());
            ev.setValue(ctx, "Note", s.note);
            Core.commit(ctx, ev);

            List<IMendixObject> children = new ArrayList<>();
            for (Snapshot.Flow f : s.flows) {
                IMendixObject o = Core.instantiate(ctx, "MemoryWatch.MemoryEventFlow");
                o.setValue(ctx, "FlowName", truncate(f.name, 500));
                o.setValue(ctx, "RunningSeconds", BigDecimal.valueOf(f.runningSeconds).setScale(1, RoundingMode.HALF_UP));
                o.setValue(ctx, "StillRunning", f.stillRunning);
                o.setValue(ctx, "MemoryWatch.MemoryEventFlow_MemoryEvent", ev.getId());
                children.add(o);
            }
            for (Snapshot.ThreadUse t : s.threads) {
                IMendixObject o = Core.instantiate(ctx, "MemoryWatch.MemoryEventThread");
                o.setValue(ctx, "ThreadName", truncate(t.name, 200));
                o.setValue(ctx, "Kind", t.kind);
                o.setValue(ctx, "AllocatedMB", t.allocatedMb);
                o.setValue(ctx, "WindowSeconds", t.windowSeconds);
                o.setValue(ctx, "MemoryWatch.MemoryEventThread_MemoryEvent", ev.getId());
                children.add(o);
            }
            if (!children.isEmpty()) {
                Core.commit(ctx, children);
            }
            ctx.endTransaction();
        } catch (Throwable t) {
            if (ctx.isInTransaction()) {
                ctx.rollbackTransaction();
            }
            throw t;
        }
    }

    private static void persistFloor(Snapshot.Floor f) throws Throwable {
        IContext ctx = Core.createSystemContext();
        IMendixObject o = Core.instantiate(ctx, "MemoryWatch.GcFloorPoint");
        o.setValue(ctx, "Timestamp", new Date(f.bucketMs));
        o.setValue(ctx, "OldGenAfterGcMB", f.usedMb);
        o.setValue(ctx, "OldGenMaxMB", f.maxMb);
        o.setValue(ctx, "UsedPercent", f.maxMb > 0 ? (int) Math.min(100, f.usedMb * 100 / f.maxMb) : 0);
        Core.commit(ctx, o);
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() > max ? s.substring(0, max) : s;
    }
}
