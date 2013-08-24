/*
 * Copyright Iconology, Inc. 2012. All rights reserved.
 */

package com.plausiblelabs.metrics.reporting;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class DummyCloudWatchClient extends AmazonCloudWatchClient {
    public final List<MetricDatum> putData = Lists.newArrayList();
    public final Map<String, MetricDatum> latestPutByName = Maps.newHashMap();

    public DummyCloudWatchClient() {
        super((AWSCredentials)null);
    }

    @Override
    public void putMetricData(PutMetricDataRequest req) throws AmazonClientException {
        putData.addAll(req.getMetricData());
        for (MetricDatum datum : req.getMetricData()) {
            latestPutByName.put(datum.getMetricName(), datum);
        }
    }
}
