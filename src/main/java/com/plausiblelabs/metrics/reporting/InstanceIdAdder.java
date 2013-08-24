/*
 * Copyright Iconology, Inc. 2012. All rights reserved.
 */

package com.plausiblelabs.metrics.reporting;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;

class InstanceIdAdder implements DimensionAdder {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceIdAdder.class);

    private final MetricFilter predicate;

    private Collection<Dimension> toSend = Collections.singletonList(new Dimension().withName("InstanceId").withValue("unknown"));
    private boolean attemptedFetchingInstanceId;
    private String instanceId;
    private long lastAttemptMillis;

    public InstanceIdAdder(MetricFilter predicate) {
        this.predicate = predicate;
    }

    public InstanceIdAdder(MetricFilter predicate, String instanceId) {
        this(predicate);
        setInstanceId(instanceId);
    }

    /**
     * Sets the InstanceId dimension sent along with the CloudWatch metrics. This will be found automatically if run
     * on EC2. If run outside EC2, this must be called or no metrics will be sent.
     */
    private void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        toSend = Collections.singletonList(new Dimension().withName("InstanceId").withValue(instanceId));
    }

    private void fetchInstanceId() {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpGet get = new HttpGet("http://169.254.169.254/latest/meta-data/instance-id");
            HttpResponse resp = httpClient.execute(get);
            if (resp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                if (!attemptedFetchingInstanceId) {
                    LOG.warn("Got bad response code {} fetching instanceId; will retry every minute till it succeeds. Metrics will be reported with the instance id 'unknown' until it succeeds. If running outside EC2, use withInstanceId(id) on the Builder.", resp.getStatusLine().getStatusCode());
                }
                return;
            }
            InputStream content = resp.getEntity().getContent();
            setInstanceId(new BufferedReader(new InputStreamReader(content, "ASCII")).readLine());
            if (attemptedFetchingInstanceId) {
                LOG.warn("Succeeded fetching instanceId after failure; the instance id will be correct now");
            }
        } catch (Exception e) {
            if (!attemptedFetchingInstanceId) {
                LOG.warn("Failed fetching instanceId; will retry every minute till it succeeds. Metrics will be reported with the instance id 'unknown' until it succeeds. If running outside EC2, use withInstanceId(id) on the Builder.", e);
            }
        } finally {
            attemptedFetchingInstanceId = true;
            lastAttemptMillis = System.currentTimeMillis();
            httpClient.getConnectionManager().shutdown();
        }
    }

    @Override
    public Collection<Dimension> generate(String name, Metric metric) {
        if (!predicate.matches(name, metric)) {
            return Collections.emptyList();
        }
        return generateJVMDimensions();
    }

    @Override
    public Collection<Dimension> generateJVMDimensions() {
        if (instanceId == null && System.currentTimeMillis() - lastAttemptMillis > 60 * 1000) {
            fetchInstanceId();
        }
        return toSend;
    }
}
