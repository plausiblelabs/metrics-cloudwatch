package com.plausiblelabs.metrics.reporting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.google.common.io.Resources;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.Timer;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class CloudWatchReporterTest {
    @Test
    public void testSendingVMMetrics() throws IOException, InterruptedException {
        URL props = Resources.getResource("aws_creds.properties");
        InputStream is = Resources.newInputStreamSupplier(props).getInput();
        AWSCredentials creds = new PropertiesCredentials(is);
        CloudWatchReporter reporter = new CloudWatchReporter(Metrics.defaultRegistry(), "cxabf", creds, MetricPredicate.ALL);
        reporter.setInstanceId("testinstance");
        Timer timer = Metrics.newTimer(CloudWatchReporterTest.class, "TestTimer", TimeUnit.MINUTES, TimeUnit.MINUTES);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 50; j++) {
                timer.update(i, TimeUnit.MINUTES);
            }
        }
        Thread.sleep(10000);
        reporter.run();
    }
}
