/*
 * Copyright Iconology, Inc. 2012. All rights reserved.
 */

package com.plausiblelabs.metrics.reporting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.google.common.io.Resources;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;

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
        Metrics.newGauge(new MetricName("test", "limits", "NegSmall"), new Gauge<Double>() {
            @Override
            public Double value() {
                return -1E-109;
            }
        });
        Metrics.newGauge(new MetricName("test", "limits", "PosSmall"), new Gauge<Double>() {
            @Override
            public Double value() {
                return 1E-109;
            }
        });
        Metrics.newGauge(new MetricName("test", "limits", "NegLarge"), new Gauge<Double>() {
            @Override
            public Double value() {
                return -CloudWatchReporter.LARGEST_SENDABLE * 10;
            }
        });
        Metrics.newGauge(new MetricName("test", "limits", "PosLarge"), new Gauge<Double>() {
            @Override
            public Double value() {
                return CloudWatchReporter.LARGEST_SENDABLE * 10;
            }
        });
        Metrics.newGauge(new MetricName("test", "limits", "NaN"), new Gauge<Double>() {
            @Override
            public Double value() {
                return Double.NaN;
            }
        });
        new CloudWatchReporter.Enabler("cxabf", creds).withEndpoint("monitoring.us-west-2.amazonaws.com")
            .withInstanceIdDimension("test").build().run();

    }
}
