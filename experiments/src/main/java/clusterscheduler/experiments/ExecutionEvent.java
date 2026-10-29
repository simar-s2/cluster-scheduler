package clusterscheduler.experiments;

/** One line of a worker's execution log. */
record ExecutionEvent(String event, String nodeId, String assignmentId, String jobId, long epoch, long timeMillis) {}
