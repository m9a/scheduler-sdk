package com.scheduler.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method to run (best-effort) when the worker terminates the container
 * (SIGTERM) before the SIGKILL grace expires — not on normal completion. Use it
 * to flush a checkpoint to {@code /workspace/output} or emit a final event. Takes
 * no parameters, or one {@code @Context TaskContext}. At most one per {@code @Job} class.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface OnShutdown {
}
