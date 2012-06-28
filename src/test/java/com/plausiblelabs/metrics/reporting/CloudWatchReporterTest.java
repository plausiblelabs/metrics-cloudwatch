package com.plausiblelabs.metrics.reporting;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.google.common.io.Resources;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;

public class CloudWatchReporterTest {
    @Test
    public void testSendingVMMetrics() throws IOException, InterruptedException {
        URL props = Resources.getResource("aws_creds.properties");
        InputStream is = Resources.newInputStreamSupplier(props).getInput();
        AWSCredentials creds = new PropertiesCredentials(is);
        CloudWatchReporter reporter = new CloudWatchReporter.Enabler("cxabf", creds)
            .withInstanceIdDimension("test")
            .withCloudWatchEnabled(false)
            .withGC(true)
            .withFiveMinuteRate(true)
            .withOneMinuteRate(false)
            .withPercentiles(.1, .5, .9, .999)
            .build();
        Timer timer = Metrics.newTimer(CloudWatchReporterTest.class, "TestTimer", TimeUnit.MINUTES, TimeUnit.MINUTES);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 50; j++) {
                timer.update(i, TimeUnit.MINUTES);
            }
        }
        Thread.sleep(1000);
        reporter.run();
    }
}
