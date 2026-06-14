package com.scheduler.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code @Task} method parameter to receive the per-task
 * {@code com.scheduler.sdk.TaskContext} — the reporting API inside a task.
 *
 * <pre>
 * {@literal @}Task(name = "extract")
 * public void extract(@Context TaskContext ctx, @Param("region") String region) { ... }
 * </pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
public @interface Context {
}
