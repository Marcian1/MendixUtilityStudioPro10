package memorywatch.implementation;

import java.util.ArrayList;
import java.util.List;

/**
 * Playground only: lets the test flows hold memory, free it, force a collection and churn garbage, so the
 * watcher can be tried against known behaviour. Nothing in the real code path uses this class.
 */
public final class TestLoad {
    private static final List<byte[]> HELD = new ArrayList<>();
    private static volatile byte[] sink;

    private TestLoad() {
    }

    /** Holds the given number of megabytes until {@link #release()} is called: memory a collection cannot free. */
    public static synchronized long hold(int megabytes) {
        for (int i = 0; i < megabytes; i++) {
            HELD.add(new byte[1024 * 1024]);
        }
        return HELD.size();
    }

    public static synchronized long release() {
        long n = HELD.size();
        HELD.clear();
        return n;
    }

    public static synchronized long heldMegabytes() {
        return HELD.size();
    }

    /** A full collection, which is what updates the old generation's "after collection" level. */
    public static void collect() {
        System.gc();
    }

    /** Allocates and drops about a megabyte at a time for the given number of seconds: lots of allocation, nothing kept. */
    public static long churn(int seconds) throws InterruptedException {
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        long chunks = 0;
        while (System.nanoTime() < end) {
            sink = new byte[1024 * 1024];
            chunks++;
            if (chunks % 8 == 0) {
                Thread.sleep(1);
            }
        }
        sink = null;
        return chunks;
    }
}
