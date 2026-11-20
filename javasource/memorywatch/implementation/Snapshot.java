package memorywatch.implementation;

import java.util.ArrayList;
import java.util.List;

/** What was seen when memory got tight. Plain data, built on the notification thread and saved by the writer. */
final class Snapshot {
    static final class Flow {
        final String name;
        final double runningSeconds;
        /** False for a flow that finished just before the note was taken. */
        final boolean stillRunning;

        Flow(String name, double runningSeconds, boolean stillRunning) {
            this.name = name;
            this.runningSeconds = runningSeconds;
            this.stillRunning = stillRunning;
        }
    }

    static final class ThreadUse {
        final String name;
        final String kind;
        final long allocatedMb;
        final int windowSeconds;

        ThreadUse(String name, String kind, long allocatedMb, int windowSeconds) {
            this.name = name;
            this.kind = kind;
            this.allocatedMb = allocatedMb;
            this.windowSeconds = windowSeconds;
        }
    }

    /** One minute of the memory floor: the lowest old-generation level after a collection. */
    static final class Floor {
        final long bucketMs;
        final long usedMb;
        final long maxMb;

        Floor(long bucketMs, long usedMb, long maxMb) {
            this.bucketMs = bucketMs;
            this.usedMb = usedMb;
            this.maxMb = maxMb;
        }
    }

    long timeMs;
    long oldUsedMb;
    long oldMaxMb;
    int percent;
    long heapUsedMb;
    long heapMaxMb;
    /** False when FlowScope is absent or off: the running flows are then unknown, not empty. */
    boolean flowsKnown;
    String note;
    final List<Flow> flows = new ArrayList<>();
    final List<ThreadUse> threads = new ArrayList<>();
}
