/*
 * Copyright Iconology, Inc. 2012. All rights reserved.
 */

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

public class RemoteCloudWatchTest {
    @Test
    public void testSendingToAmazon() throws IOException {
        URL props = Resources.getResource("aws_creds.properties");
        InputStream is = Resources.newInputStreamSupplier(props).getInput();
        AWSCredentials creds = new PropertiesCredentials(is);

        Timer timer = Metrics.newTimer(CloudWatchReporterTest.class, "TestTimer", TimeUnit.MINUTES, TimeUnit.MINUTES);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 50; j++) {
                timer.update(i, TimeUnit.MINUTES);
            }
        }
        new CloudWatchReporter.Enabler("cxabf", creds)
            .withInstanceIdDimension("test").build().run();

    }
}
