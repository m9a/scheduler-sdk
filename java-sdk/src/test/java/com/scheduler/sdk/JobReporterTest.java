package com.scheduler.sdk;

import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.TaskState;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobReporterTest {

    private static final byte TYPE_TAG_STATUS = 0x01;
    private static final byte TYPE_TAG_ACK = 0x02;

    /** Stands in for the worker's JobCallbackServer: records status frames and acks each one. */
    private static final class FakeWorker extends WebSocketServer {
        final List<StatusUpdate> statuses = new CopyOnWriteArrayList<>();
        final CountDownLatch ready = new CountDownLatch(1);

        FakeWorker(int port) {
            super(new InetSocketAddress("localhost", port));
            setReuseAddr(true);
        }

        @Override
        public void onMessage(WebSocket conn, ByteBuffer buffer) {
            try {
                byte tag = buffer.get();
                byte[] payload = new byte[buffer.remaining()];
                buffer.get(payload);
                if (tag == TYPE_TAG_STATUS) {
                    statuses.add(StatusUpdate.parseFrom(payload));
                    conn.send(new byte[]{TYPE_TAG_ACK});
                }
                // liveness/telemetry: not acked, ignored
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onStart() {
            ready.countDown();
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {}

        @Override
        public void onMessage(WebSocket conn, String message) {}

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {}

        @Override
        public void onError(WebSocket conn, Exception ex) {}

        static FakeWorker start(int port) throws InterruptedException {
            FakeWorker worker = new FakeWorker(port);
            worker.start();
            assertTrue(worker.ready.await(5, TimeUnit.SECONDS), "server failed to start");
            return worker;
        }
    }

    // Worker restarts mid-job: the completed update queues while the worker is down
    // (task thread not blocked), then the reconnect loop delivers it to the new server.
    @Test
    void testReconnectAfterWorkerRestart() throws Exception {
        FakeWorker worker = FakeWorker.start(0);
        int port = worker.getPort();
        JobReporter reporter = JobReporter.connect("ws://localhost:" + port, "job-1");
        try {
            reporter.taskStarted(0, "train");
            waitFor(() -> worker.statuses.size() == 1);

            worker.stop(1000);  // worker "crashes"
            long enqueueStart = System.currentTimeMillis();
            reporter.taskCompleted(0, "train", "done");  // queued; must not block the task thread
            assertTrue(System.currentTimeMillis() - enqueueStart < 2000,
                    "status send blocked the task thread");

            FakeWorker restarted = FakeWorker.start(port);  // worker comes back on the same port
            try {
                reporter.close();  // drains the queue via the reconnect loop
                assertEquals(1, restarted.statuses.size());
                StatusUpdate delivered = restarted.statuses.get(0);
                assertEquals(TaskState.TASK_STATE_COMPLETED, delivered.getTaskState());
                assertEquals("job-1", delivered.getJobId());
            } finally {
                restarted.stop(1000);
            }
        } finally {
            worker.stop(1000);
        }
    }

    // Happy path: both updates delivered in order, close returns with nothing queued.
    @Test
    void testStatusDeliveredInOrder() throws Exception {
        FakeWorker worker = FakeWorker.start(0);
        try {
            JobReporter reporter = JobReporter.connect("ws://localhost:" + worker.getPort(), "job-1");
            reporter.taskStarted(0, "train");
            reporter.taskCompleted(0, "train", "done");
            reporter.close();
            assertEquals(2, worker.statuses.size());
            assertEquals(TaskState.TASK_STATE_RUNNING, worker.statuses.get(0).getTaskState());
            assertEquals(TaskState.TASK_STATE_COMPLETED, worker.statuses.get(1).getTaskState());
        } finally {
            worker.stop(1000);
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 5s");
            }
            Thread.sleep(20);
        }
    }
}
