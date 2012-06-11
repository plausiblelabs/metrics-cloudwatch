package com.plausiblelabs.metrics.reporting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.google.common.io.Resources;
import com.yammer.metrics.Metrics;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class CloudWatchReporterTest {
    @Test
    public void testSendingVMMetrics() throws IOException {
        URL props = Resources.getResource("aws_creds.properties");
        InputStream is = Resources.newInputStreamSupplier(props).getInput();
        AWSCredentials creds = new PropertiesCredentials(is);
        CloudWatchReporter reporter = new CloudWatchReporter(Metrics.defaultRegistry(), "cxabf", creds);
        reporter.run();
    }
}
