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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CloudWatchReporter extends AbstractPollingReporter {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class);
    
    private final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
    private final List<Dimension> instanceDimensions = new ArrayList<Dimension>();
    private final String namespace;
    private final AmazonCloudWatchClient client;

    private PutMetricDataRequest putReq;

    public CloudWatchReporter(MetricsRegistry registry, String namespace, AWSCredentials creds) {
        super(registry, "cloudwatch-reporter");

        this.namespace = namespace;
        client = new AmazonCloudWatchClient(creds);
    }

    @Override
    public void run() {
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
