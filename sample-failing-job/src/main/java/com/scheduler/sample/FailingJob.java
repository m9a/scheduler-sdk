package com.scheduler.sample;

import com.scheduler.annotation.Context;
import com.scheduler.annotation.Job;
import com.scheduler.annotation.Task;
import com.scheduler.sdk.TaskContext;

/**
 * Sample job with a task that deliberately fails.
 * Used by integration tests to verify task-failure propagation.
 */
@Job(id = "failing-job", description = "Job with a task that deliberately fails")
public class FailingJob {

    public FailingJob() {}

    @Task(name = "validate", order = 1)
    public void validate(@Context TaskContext ctx) {
        System.out.println("Validating data — this task succeeds");
    }

    @Task(name = "process", order = 2)
    public void process(@Context TaskContext ctx) {
        throw new RuntimeException("Intentional failure for testing");
    }

    @Task(name = "finalize", order = 3)
    public void finalize_step(@Context TaskContext ctx) {
        System.out.println("This task should never run");
    }
}
