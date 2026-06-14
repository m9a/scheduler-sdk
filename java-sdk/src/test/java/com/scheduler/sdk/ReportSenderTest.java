package com.scheduler.sdk;

import com.scheduler.proto.job.Report;
import com.scheduler.proto.v1.ReportEntry;
import com.scheduler.proto.v1.ReportKind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportSenderTest {

    @Test
    void testEventFlushesImmediately() {
        List<byte[]> sent = new ArrayList<>();
        ReportSender sender = new ReportSender(sent::add, "job-1");

        sender.reportStr(0, "checkpoint_saved", ReportKind.REPORT_KIND_EVENT, "step 5000", true);

        List<Report> reports = decode(sent);
        assertEquals(1, reports.size());
        assertEquals("checkpoint_saved", reports.get(0).getEntries(0).getKey());
        assertEquals("step 5000", reports.get(0).getEntries(0).getStrValue());
    }

    @Test
    void testThrottleLatestWinsThenFlush() {
        List<byte[]> sent = new ArrayList<>();
        ReportSender sender = new ReportSender(sent::add, "job-1");

        // Not forced and within the 1s window → buffered, nothing sent yet.
        sender.reportNum(0, "loss", ReportKind.REPORT_KIND_METRIC, 0.5, false);
        sender.reportNum(0, "loss", ReportKind.REPORT_KIND_METRIC, 0.2, false);
        assertTrue(sent.isEmpty());

        sender.flush();
        List<Report> reports = decode(sent);
        assertEquals(1, reports.size());
        assertEquals(1, reports.get(0).getEntriesCount());        // latest-wins per key
        assertEquals(0.2, reports.get(0).getEntries(0).getNumValue());
    }

    @Test
    void testTaskContextProgress() {
        List<byte[]> sent = new ArrayList<>();
        TaskContext ctx = new TaskContext(new ReportSender(sent::add, "job-1"), 2, "train");

        ctx.progress(3, 10);
        ctx.event("tick", "");  // forces a flush of everything buffered for the task

        Report report = decode(sent).get(0);
        assertEquals(2, report.getTaskIndex());
        Map<String, ReportEntry> byKey = new HashMap<>();
        report.getEntriesList().forEach(e -> byKey.put(e.getKey(), e));
        assertEquals(3, byKey.get("progress_current").getNumValue());
        assertEquals(10, byKey.get("progress_total").getNumValue());
    }

    private static List<Report> decode(List<byte[]> sent) {
        List<Report> reports = new ArrayList<>();
        for (byte[] frame : sent) {
            assertEquals(ReportSender.TYPE_TAG_REPORT, frame[0]);
            try {
                reports.add(Report.parseFrom(java.util.Arrays.copyOfRange(frame, 1, frame.length)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return reports;
    }
}
