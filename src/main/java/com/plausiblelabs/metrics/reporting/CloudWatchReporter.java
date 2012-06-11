package com.plausiblelabs.metrics.reporting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CloudWatchReporter extends AbstractPollingReporter {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class);
    
    private final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
    private final List<Dimension> instanceDimensions = new ArrayList<Dimension>();
    private final String namespace;
    private final AmazonCloudWatchClient client;
    private final boolean sendInstanceId;

    private boolean attemptedFetchingInstanceId;
    private String instanceId;


    private PutMetricDataRequest putReq;

    public CloudWatchReporter(MetricsRegistry registry, String namespace, AWSCredentials creds, boolean sendInstanceId) {
        super(registry, "cloudwatch-reporter");

        this.namespace = namespace;
        this.sendInstanceId = sendInstanceId;
        client = new AmazonCloudWatchClient(creds);
    }

    @Override
    public void run() {
        if (sendInstanceId && instanceId == null) {
            fetchInstanceId();
            instanceDimensions.add(new Dimension().withName("InstanceId").withValue(instanceId));
            if (instanceId == null) {
                return;
            }
        }

        putReq = new PutMetricDataRequest().withNamespace(namespace);
        try {
            Date timestamp = new Date();
            sendVMMetrics(timestamp);
            client.putMetricData(putReq);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to CloudWatch", e);
            } else {
                LOG.warn("Error writing to CloudWatch: {}", e.getMessage());
            }
        } finally {
            putReq = null;
        }
    }

    private void fetchInstanceId() {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpGet get = new HttpGet("http://169.254.169.254/latest/meta-data/instance-id");
            HttpResponse resp = httpClient.execute(get);
            if (resp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                if (!attemptedFetchingInstanceId) {
                    LOG.warn("Got bad response code {} fetching instanceId; will retry on next scheduled report. No metrics will be reported until it succeeds.", resp.getStatusLine().getStatusCode());
                }
                return;
            }
            InputStream content = resp.getEntity().getContent();
            instanceId = new BufferedReader(new InputStreamReader(content, "ASCII")).readLine();
            if (attemptedFetchingInstanceId) {
                LOG.warn("Succeeded fetching instanceId after failure; all new metrics will now be reported");
            }
        } catch (Exception e) {
            if (!attemptedFetchingInstanceId) {
                LOG.warn("Failed fetching instanceId; will retry on next scheduled report. No metrics will be reported until it succeeds.", e);
            }
        } finally {
            attemptedFetchingInstanceId = true;
            httpClient.getConnectionManager().shutdown();
        }
    }

    protected void sendVMMetrics(Date timestamp) {
        sendInstanceSpecificValue(timestamp, "jvm.memory.heap_usage", vm.heapUsage(), StandardUnit.Percent);
        sendInstanceSpecificValue(timestamp, "jvm.thread_count", vm.threadCount(), StandardUnit.Count);
        sendInstanceSpecificValue(timestamp, "jvm.fd_usage", vm.fileDescriptorUsage(), StandardUnit.Percent);
    }

    private final void sendInstanceSpecificValue(Date timestamp, String name, double value, StandardUnit unit) {
        MetricDatum datum = new MetricDatum()
                .withTimestamp(timestamp)
                .withValue(value)
                .withMetricName(name)
                .withDimensions(instanceDimensions)
                .withUnit(unit);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending {}", datum);
        }
        putReq.withMetricData(datum);
    }
}
