package io.kafkadlq.server.model;

import java.util.List;

/**
 * Outcome of a replay (or dry-run) request.
 */
public record ReplayResult(
        boolean dryRun,
        int succeeded,
        int failed,
        int skipped,
        List<ItemResult> items) {

    public record ItemResult(
            int partition,
            long offset,
            Status status,
            String targetTopic,
            String detail) {

        public enum Status { REPLAYED, WOULD_REPLAY, FAILED, SKIPPED }

        public static ItemResult replayed(ReplayRequest.MessageCoordinates c, String target) {
            return new ItemResult(c.partition(), c.offset(), Status.REPLAYED, target, null);
        }

        public static ItemResult wouldReplay(ReplayRequest.MessageCoordinates c, String target) {
            return new ItemResult(c.partition(), c.offset(), Status.WOULD_REPLAY, target, null);
        }

        public static ItemResult failed(ReplayRequest.MessageCoordinates c, String detail) {
            return new ItemResult(c.partition(), c.offset(), Status.FAILED, null, detail);
        }

        public static ItemResult skipped(ReplayRequest.MessageCoordinates c, String detail) {
            return new ItemResult(c.partition(), c.offset(), Status.SKIPPED, null, detail);
        }
    }
}
