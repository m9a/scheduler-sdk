package com.scheduler.sample;

import com.scheduler.annotation.AfterJob;
import com.scheduler.annotation.BeforeJob;
import com.scheduler.annotation.Context;
import com.scheduler.annotation.Job;
import com.scheduler.annotation.OnShutdown;
import com.scheduler.annotation.Param;
import com.scheduler.annotation.Task;
import com.scheduler.sdk.TaskContext;

/**
 * Sample annotated job demonstrating the annotation-based API.
 * Simulates a daily sales ETL pipeline: extract → transform → load.
 */
@Job(id = "daily-sales-etl",
     description = "Daily sales ETL pipeline",
     timeoutSeconds = 3600,
     maxRetries = 2)
public class DailySalesEtlJob {

    private final String region;
    private final int batchSize;

    // Inter-task data lives on the job instance — tasks are methods on the same object.
    private int rowCount;

    public DailySalesEtlJob(@Param("region") String region,
                            @Param(value = "batchSize", defaultValue = "1000") int batchSize) {
        this.region = region;
        this.batchSize = batchSize;
    }

    @BeforeJob
    public void setup() {
        System.out.println("Setting up ETL for region=" + region + ", batchSize=" + batchSize);
    }

    @Task(name = "extract", order = 1, critical = true)
    public void extract(@Context TaskContext ctx) {
        System.out.println("Extracting sales data for region=" + region);
        rowCount = 5000;
        ctx.metric("rows_extracted", rowCount);
    }

    @Task(name = "transform", order = 2, dependsOn = "extract")
    public void transform(@Context TaskContext ctx) {
        System.out.println("Transforming " + rowCount + " rows in batches of " + batchSize);
        long start = System.currentTimeMillis();
        int batches = (rowCount + batchSize - 1) / batchSize;
        for (int i = 0; i < batches; i++) {
            ctx.progress(i + 1, batches);
        }
        ctx.metric("transform_duration_ms", System.currentTimeMillis() - start);
    }

    @Task(name = "load", order = 3, dependsOn = "transform")
    public void load(@Context TaskContext ctx) {
        System.out.println("Loading " + rowCount + " rows into warehouse");
        ctx.event("load_complete", rowCount + " rows");
    }

    @AfterJob
    public void cleanup() {
        System.out.println("Cleaning up temporary files");
    }

    // Runs only if the worker terminates the container mid-run (SIGTERM).
    @OnShutdown
    public void onShutdown(@Context TaskContext ctx) {
        System.out.println("Shutting down — flushing partial progress for " + rowCount + " rows");
        ctx.event("shutdown", "terminated after " + rowCount + " rows");
    }
}
