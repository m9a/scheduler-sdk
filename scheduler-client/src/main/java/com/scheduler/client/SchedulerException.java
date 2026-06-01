package com.scheduler.client;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Wraps gRPC {@link StatusRuntimeException} with a human-readable message
 * and easy access to the gRPC status code. Thrown by all public methods on
 * {@link SchedulerClient}.
 */
public class SchedulerException extends RuntimeException {

    private final Status.Code code;

    public SchedulerException(StatusRuntimeException cause) {
        super(formatMessage(cause), cause);
        this.code = cause.getStatus().getCode();
    }

    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
        this.code = Status.Code.UNKNOWN;
    }

    public Status.Code getCode() {
        return code;
    }

    private static String formatMessage(StatusRuntimeException e) {
        String description = e.getStatus().getDescription();
        if (description != null && !description.isEmpty()) {
            return description;
        }
        return e.getStatus().getCode().name();
    }
}
