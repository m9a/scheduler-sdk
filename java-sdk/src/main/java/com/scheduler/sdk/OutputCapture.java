package com.scheduler.sdk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Per-task stdout/stderr capture used by the generated {@code _Harness}.
 * {@link #start()} swaps {@code System.out/err} for a tee that writes to both the
 * real console (so the worker still sees live logs) and an in-memory buffer;
 * {@link #stop()} restores the streams and returns the captured text, which the
 * harness attaches to the task's COMPLETED/FAILED status.
 *
 * <p>Swaps the global streams, so it assumes tasks run one at a time (they do —
 * the harness runs tasks sequentially).
 */
public final class OutputCapture {

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private PrintStream originalOut;
    private PrintStream originalErr;

    public void start() {
        originalOut = System.out;
        originalErr = System.err;
        System.setOut(new PrintStream(new TeeOutputStream(originalOut, buffer), true));
        System.setErr(new PrintStream(new TeeOutputStream(originalErr, buffer), true));
    }

    /** Restores the original streams and returns captured text, or null if nothing was printed. */
    public String stop() {
        if (originalOut != null) {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
        return buffer.size() == 0 ? null : buffer.toString();
    }

    /** Writes to both the original stream and the capture buffer simultaneously. */
    private static final class TeeOutputStream extends OutputStream {

        private final OutputStream original;
        private final OutputStream capture;

        TeeOutputStream(OutputStream original, OutputStream capture) {
            this.original = original;
            this.capture = capture;
        }

        @Override
        public void write(int b) throws IOException {
            original.write(b);
            capture.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            original.write(b, off, len);
            capture.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            original.flush();
            capture.flush();
        }
    }
}
