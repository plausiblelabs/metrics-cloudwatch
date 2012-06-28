/*
 * Copyright Iconology, Inc. 2012. All rights reserved.
 */

package com.plausiblelabs.metrics.reporting;

import java.util.Collection;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

public interface DimensionAdder {
    Collection<Dimension> generate(MetricName name, Metric metric);
    Collection<Dimension> generateJVMDimensions();
}
