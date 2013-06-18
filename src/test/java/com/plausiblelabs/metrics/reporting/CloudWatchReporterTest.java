package com.plausiblelabs.metrics.reporting;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class CloudWatchReporterTest {
    // Use a separate registry for each test to keep the metrics apart
    private MetricsRegistry testRegistry = new MetricsRegistry();
    private DummyCloudWatchClient client = new DummyCloudWatchClient();
    private CloudWatchReporter.Enabler enabler =
        new CloudWatchReporter.Enabler("testnamespace", client).withRegistry(testRegistry)
            .withEndpoint("monitoring.us-west-2.amazonaws.com");

    @After
    public void shutdownRegistry() {
        testRegistry.shutdown();
    }

    @Test
    public void testDefaultSentMetrics() throws IOException, InterruptedException {
        enabler.build().run();
        assertEquals(2, client.putData.size());
        for (MetricDatum datum : client.putData) {
            assertTrue(datum.getDimensions().isEmpty());
            assertTrue(datum.getMetricName().startsWith("jvm.memory"));
        }
    }

    @Test
    public void testInstanceIdDimension() throws IOException, InterruptedException {
        enabler.withInstanceIdDimension("flask").build().run();
        assertEquals(2, client.putData.size());
        for (MetricDatum datum : client.putData) {
            assertEquals(1, datum.getDimensions().size());
            assertEquals("InstanceId", datum.getDimensions().get(0).getName());
            assertEquals("flask", datum.getDimensions().get(0).getValue());
        }
    }

    @Test
    public void testDisablingDefaults() throws IOException, InterruptedException {
        enabler.withJVMMemory(false).build().run();
        assertEquals(0, client.putData.size());
    }

    @Test
    public void testAllJVMMetricsSent() throws IOException, InterruptedException {
        enabler.withJVMThreadState(true).withJVMGC(true).build().run();
        assertEquals(2 + // memory metrics
            Thread.State.values().length + 2 + // Thread metrics
            VirtualMachineMetrics.getInstance().garbageCollectors().size() * 2, // GC metrics
            client.putData.size());
    }

    @Test
    public void testTimer() {
        enabler
            .withJVMMemory(false)
            .withFiveMinuteRate(true)
            .withOneMinuteRate(false)
            .withTimerSummary(true)
            .withPercentiles(.1, .5, .9, .999);
        Timer timer = testRegistry.newTimer(CloudWatchReporterTest.class, "TestTimer", TimeUnit.MINUTES, TimeUnit.MINUTES);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 50; j++) {
                timer.update(i, TimeUnit.MINUTES);
            }
        }
        enabler.build().run();
        assertEquals(9, client.putData.size());
        assertEquals(Sets.newHashSet("com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.median",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.999",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.9",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.mean",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.5MinuteRate",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.min",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.max",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.stddev",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.1"),
                     client.latestPutByName.keySet());
        MetricDatum min = client.latestPutByName.get("com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.min");
        assertEquals("The recorded minutes were converted to seconds for CloudWatch", StandardUnit.Seconds.toString(), min.getUnit());
        assertEquals(0.0, min.getValue());
        MetricDatum percentile999 = client.latestPutByName.get("com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.999");
        assertEquals("The recorded minutes were converted to seconds for CloudWatch", 5940.0, percentile999.getValue());
    }

    @Test
    public void testUnsupportedGaugeType() {
        testRegistry.newGauge(CloudWatchReporterTest.class, "TestGague", new Gauge<String>() {
            @Override
            public String value() {
                return "A value!";
            }
        });
        enabler.withJVMMemory(false).build().run();
        assertEquals(0, client.putData.size());
    }

    @Test
    public void testSupportedGaugeType() {
        testRegistry.newGauge(CloudWatchReporterTest.class, "TestGague", new Gauge<Double>() {
            @Override
            public Double value() {
                return 5.0;
            }
        });
        enabler.withJVMMemory(false).build().run();
        assertEquals(1, client.putData.size());
    }

    @Test
    public void testCounter() {
        Counter counter = testRegistry.newCounter(CloudWatchReporterTest.class, "TestCounter");
        CloudWatchReporter reporter = enabler.withJVMMemory(false).build();
        reporter.run();
        assertEquals(1, client.putData.size());
        assertEquals(0.0, client.putData.get(0).getValue());
        assertEquals(StandardUnit.Count.toString(), client.putData.get(0).getUnit());
        counter.inc();
        client.putData.clear();
        reporter.run();
        assertEquals(1.0, client.putData.get(0).getValue());

    }
}
