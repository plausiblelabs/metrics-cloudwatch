package com.plausiblelabs.metrics.reporting;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

public class CloudWatchReporter extends AbstractPollingReporter implements MetricProcessor<Date> {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class);

    public static class Enabler {
        private final String namespace;
        private final AmazonCloudWatchClient client;
        private final List<DimensionAdder> dimensionAdders = new ArrayList<DimensionAdder>();

        private MetricsRegistry registry = Metrics.defaultRegistry();
        private MetricPredicate predicate = MetricPredicate.ALL;

        private long period = 1;
        private TimeUnit unit = TimeUnit.MINUTES;

        private boolean sendToCloudWatch = true;

        private double[] percentilesToSend = {.5, .95, .99};
        private boolean sendOneMinute = true, sendFiveMinute, sendFifteenMinute;
        private boolean sendMeterSummary;
        private boolean sendTimerLifetime;
        private boolean sendHistoLifetime;
        private boolean sendJVMMemory = true;
        private boolean sendJVMThreadState;
        private boolean sendGC;

        public Enabler withPercentiles(double...percentiles) {
            this.percentilesToSend = percentiles;
            return this;
        }

        public Enabler withOneMinuteRate(boolean enabled) {
            this.sendOneMinute = enabled;
            return this;
        }

        public Enabler withFiveMinuteRate(boolean enabled) {
            this.sendFiveMinute = enabled;
            return this;
        }

        public Enabler withFifteenMinuteRate(boolean enabled) {
            this.sendFifteenMinute = enabled;
            return this;
        }

        public Enabler withMeterSummary(boolean enabled) {
            this.sendMeterSummary = enabled;
            return this;
        }

        public Enabler withTimerSummary(boolean enabled) {
            this.sendTimerLifetime = enabled;
            return this;
        }

        public Enabler withHistogramSummary(boolean enabled) {
            this.sendHistoLifetime = enabled;
            return this;
        }

        public Enabler withJVMMemory(boolean enabled) {
            this.sendJVMMemory = enabled;
            return this;
        }

        public Enabler withJVMThreadState(boolean enabled) {
            this.sendJVMThreadState = enabled;
            return this;
        }

        public Enabler withJVMGC(boolean enabled) {
            this.sendGC = enabled;
            return this;
        }

        public Enabler(String namespace, AWSCredentials creds) {
            this(namespace, new AmazonCloudWatchClient(creds));
        }

        public Enabler(String namespace, AmazonCloudWatchClient client) {
            this.namespace = namespace;
            this.client = client;
        }

        public Enabler withRegistry(MetricsRegistry registry) {
            this.registry = registry;
            return this;
        }

        public Enabler withPredicate(MetricPredicate predicate) {
            this.predicate = predicate;
            return this;
        }

        public Enabler withDelay(long period, TimeUnit unit) {
            this.period = period;
            this.unit = unit;
            return this;
        }

        public Enabler withEC2InstanceIdDimension() {
            return withEC2InstanceIdDimension(MetricPredicate.ALL);
        }

        public Enabler withEC2InstanceIdDimension(MetricPredicate predicate) {
            return withDimensionAdder(new InstanceIdAdder(predicate));
        }

        public Enabler withInstanceIdDimension(String instanceId) {
            return withInstanceIdDimension(instanceId, MetricPredicate.ALL);
        }

        public Enabler withInstanceIdDimension(String instanceId, MetricPredicate predicate) {
            return withDimensionAdder(new InstanceIdAdder(predicate, instanceId));
        }

        public Enabler withDimensionAdder(DimensionAdder adder) {
            this.dimensionAdders.add(adder);
            return this;
        }

        public Enabler withCloudWatchEnabled(boolean enabled) {
            sendToCloudWatch = enabled;
            return this;
        }


        public CloudWatchReporter build() {
            return new CloudWatchReporter(registry, namespace, client, predicate, dimensionAdders,
                                          sendToCloudWatch, percentilesToSend, sendOneMinute, sendFiveMinute,
                                          sendFifteenMinute, sendMeterSummary, sendTimerLifetime, sendHistoLifetime,
                                          sendJVMMemory, sendJVMThreadState, sendGC);
        }

        public void enable() {
            try {
                build().start(period, unit);
            } catch (Exception e) {
                LOG.error("Error creating/starting CloudWatch reporter:", e);
            }
        }
    }

    private final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
    private final List<DimensionAdder> dimensionAdders;
    private final Set<MetricName> unsendable = new HashSet<MetricName>();
    private final Set<MetricName> nonCloudWatchUnit = new HashSet<MetricName>();
    private final MetricPredicate predicate;
    private final String namespace;
    private final AmazonCloudWatchClient client;
    private final boolean sendToCloudWatch;

    private final double[] percentilesToSend;
    private final boolean sendOneMinute, sendFiveMinute, sendFifteenMinute;
    private final boolean sendMeterSummary;
    private final boolean sendTimerLifetime;
    private final boolean sendHistoLifetime;
    private final boolean sendJVMMemory;
    private final boolean sendJVMThreads;
    private final boolean sendJVMGC;

    private PutMetricDataRequest putReq;

    private CloudWatchReporter(MetricsRegistry registry, String namespace, AmazonCloudWatchClient client,
                               MetricPredicate predicate, List<DimensionAdder> dimensionAdders,
                               boolean sendToCloudWatch, double[] percentilesToSend, boolean sendOneMinute,
                               boolean sendFiveMinute, boolean sendFifteenMinute, boolean sendMeterSummary,
                               boolean sendTimerLifetime, boolean sendHistoLifetime, boolean sendJVMMemory,
                               boolean sendJVMThreads, boolean sendJVMGC) {
        super(registry, "cloudwatch-reporter");
        this.predicate = predicate;

        this.namespace = namespace;
        this.client = client;
        this.dimensionAdders = dimensionAdders;
        this.sendToCloudWatch = sendToCloudWatch;

        this.percentilesToSend = percentilesToSend;
        this.sendOneMinute = sendOneMinute;
        this.sendFiveMinute = sendFiveMinute;
        this.sendFifteenMinute = sendFifteenMinute;
        this.sendMeterSummary = sendMeterSummary;
        this.sendTimerLifetime = sendTimerLifetime;
        this.sendHistoLifetime = sendHistoLifetime;
        this.sendJVMMemory = sendJVMMemory;
        this.sendJVMThreads = sendJVMThreads;
        this.sendJVMGC = sendJVMGC;
    }

    @Override
    public void run() {
        putReq = new PutMetricDataRequest().withNamespace(namespace);
        try {
            Date timestamp = new Date();
            sendVMMetrics(timestamp);
            sendRegularMetrics(timestamp);
            sendToCloudWatch();
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

    private void sendToCloudWatch() {
        if (sendToCloudWatch && !putReq.getMetricData().isEmpty()) {
            client.putMetricData(putReq);
        }
        putReq = new PutMetricDataRequest().withNamespace(namespace);
    }

    private void sendValue(Date timestamp, String name, double value, StandardUnit unit, List<Dimension> dimensions) {
        // TODO limit to 10 dimensions
        MetricDatum datum = new MetricDatum()
            .withTimestamp(timestamp)
            .withValue(value)
            .withMetricName(name)
            .withDimensions(dimensions)
            .withUnit(unit);



        if (!sendToCloudWatch) {
            LOG.info("Not sending {}", datum);
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Sending {}", datum);
        }
        putReq.withMetricData(datum);

        // Can only send 20 metrics at a time
        if (putReq.getMetricData().size() == 20) {
            sendToCloudWatch();
        }
    }

    private void sendRegularMetrics(Date timestamp) {
        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(predicate).entrySet()) {
            for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), timestamp);
                    } catch (Exception ignored) {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }
    }


    private void sendVMMetrics(Date timestamp) {
        List<Dimension> dimensions = new ArrayList<Dimension>();
        for (DimensionAdder adder : dimensionAdders) {
            dimensions.addAll(adder.generateJVMDimensions());
        }
        if (sendJVMMemory) {
            sendValue(timestamp, "jvm.memory.heap_usage", vm.heapUsage(), StandardUnit.Percent, dimensions);
            sendValue(timestamp, "jvm.memory.non_heap_usage", vm.nonHeapUsage(), StandardUnit.Percent, dimensions);
        }

        if (sendJVMThreads) {
            sendValue(timestamp, "jvm.thread_count", vm.threadCount(), StandardUnit.Count, dimensions);
            sendValue(timestamp, "jvm.daemon_thread_count", vm.daemonThreadCount(), StandardUnit.Count, dimensions);
            for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
                sendValue(timestamp, "jvm.thread-states." + entry.getKey().toString().toLowerCase(), entry.getValue(), StandardUnit.Count, dimensions);
            }
        }

        if (sendJVMGC) {
            for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
                sendValue(timestamp, "jvm.gc." + entry.getKey() + ".time", entry.getValue().getTime(TimeUnit.MILLISECONDS), StandardUnit.Milliseconds, dimensions);
                sendValue(timestamp, "jvm.gc." + entry.getKey() + ".runs", entry.getValue().getRuns(), StandardUnit.Count, dimensions);
            }
        }
    }

    private List<Dimension> createDimensions(MetricName name, Metric metric) {
        List<Dimension> dimensions = new ArrayList<Dimension>();
        for (DimensionAdder adder : dimensionAdders) {
            dimensions.addAll(adder.generate(name, metric));
        }
        return dimensions;
    }

    private String sanitizeName(MetricName name) {
        // TODO - collapse package names if the name is over 255 characters
        final StringBuilder sb = new StringBuilder()
            .append(name.getGroup())
            .append('.')
            .append(name.getType())
            .append('.');
        if (name.hasScope()) {
            sb.append(name.getScope())
                .append('.');
        }
        return sb.append(name.getName()).toString();
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Date context) throws Exception {
        if (gauge.value() instanceof Number) {
            sendValue(context, sanitizeName(name), ((Number) gauge.value()).doubleValue(), StandardUnit.None, createDimensions(name, gauge));
        } else if (unsendable.add(name)) {
            LOG.warn("The type of the value for {} is {}. It must be a subclass of Number to send to CloudWatch.", name, gauge.value().getClass());
        }
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Date context) throws Exception {
        sendValue(context, sanitizeName(name), counter.count(), StandardUnit.Count, createDimensions(name, counter));
    }


    @Override
    public void processMeter(MetricName name, Metered meter, Date context) throws Exception {
        List<Dimension> dimensions = createDimensions(name, meter);
        String sanitizedName = sanitizeName(name);
        String rateUnits = meter.rateUnit().name();
        String rateUnit = rateUnits.substring(0, rateUnits.length() - 1).toLowerCase(Locale.US);
        // CloudWatch only supports its standard units, so this rate won't line up. Instead send the unit as a dimension and call the unit None.
        dimensions.add(new Dimension().withName("meterUnit").withValue(meter.eventType() + '/' + rateUnit));
        if (sendOneMinute) {
            sendValue(context, sanitizedName + ".1MinuteRate", meter.oneMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendFiveMinute) {
            sendValue(context, sanitizedName + ".5MinuteRate", meter.fiveMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendFifteenMinute) {
            sendValue(context, sanitizedName + ".15MinuteRate", meter.fifteenMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendMeterSummary) {
            sendValue(context, sanitizedName + ".count", meter.count(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".meanRate", meter.meanRate(), StandardUnit.None, dimensions);
        }
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Date context) throws Exception {
        List<Dimension> dimensions = createDimensions(name, histogram);
        String sanitizedName = sanitizeName(name);
        Snapshot snapshot = histogram.getSnapshot();
        for (double percentile : percentilesToSend) {
            if (percentile == .5) {
                sendValue(context, sanitizedName + ".median", snapshot.getMedian(), StandardUnit.None, dimensions);
            } else {
                sendValue(context, sanitizedName + "_percentile_" + percentile, snapshot.getValue(percentile), StandardUnit.None, dimensions);
            }
        }
        if (sendHistoLifetime) {
            sendValue(context, sanitizedName + ".min", histogram.min(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".max", histogram.max(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".mean", histogram.mean(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".stddev", histogram.stdDev(), StandardUnit.None, dimensions);
        }
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Date context) throws Exception {
        // Convert the timer's duration unit to a unit cloud watch understands if possible
        TimeUnit recordedUnit = timer.durationUnit();
        StandardUnit cloudWatchUnit;
        TimeUnit sendUnit;
        switch(recordedUnit) {
        case MILLISECONDS:
            cloudWatchUnit = StandardUnit.Milliseconds;
            sendUnit = TimeUnit.MILLISECONDS;
            break;
        case MICROSECONDS:
            cloudWatchUnit = StandardUnit.Microseconds;
            sendUnit = TimeUnit.MICROSECONDS;
            break;
        case SECONDS:
            cloudWatchUnit = StandardUnit.Seconds;
            sendUnit = TimeUnit.SECONDS;
            break;
        case NANOSECONDS:
            if (nonCloudWatchUnit.add(name)) {
                LOG.debug("Cloud Watch doesn't support nanosecond units; converting {} to microseconds.", name);
            }
            cloudWatchUnit = StandardUnit.Microseconds;
            sendUnit = TimeUnit.MICROSECONDS;
            break;
        case DAYS:
        case HOURS:
        case MINUTES:
            if (nonCloudWatchUnit.add(name)) {
                LOG.debug("Cloud Watch doesn't support {} units; converting {} to seconds.", timer.durationUnit(), name);
            }
            cloudWatchUnit = StandardUnit.Seconds;
            sendUnit = TimeUnit.SECONDS;
            break;
        default:
            if (nonCloudWatchUnit.add(name)) {
                LOG.warn("Unknown TimeUnit {}; not sending {}.", timer.durationUnit(), name);
            }
            return;
        }

        processMeter(name, timer, context);

        List<Dimension> dimensions = createDimensions(name, timer);
        String sanitizedName = sanitizeName(name);
        Snapshot snapshot = timer.getSnapshot();
        for (double percentile : percentilesToSend) {
            if (percentile == .5) {
                sendValue(context, sanitizedName + ".median", convertIfNecessary(snapshot.getMedian(), recordedUnit, sendUnit), cloudWatchUnit, dimensions);
            } else {
                sendValue(context, sanitizedName + "_percentile_" + percentile, convertIfNecessary(snapshot.getValue(percentile), recordedUnit, sendUnit), cloudWatchUnit, dimensions);
            }
        }
        if (sendTimerLifetime) {
            sendValue(context, sanitizedName + ".min", convertIfNecessary(timer.min(), recordedUnit, sendUnit), cloudWatchUnit, dimensions);
            sendValue(context, sanitizedName + ".max", convertIfNecessary(timer.max(), recordedUnit, sendUnit), cloudWatchUnit, dimensions);
            sendValue(context, sanitizedName + ".mean", convertIfNecessary(timer.mean(), recordedUnit, sendUnit), cloudWatchUnit, dimensions);
            sendValue(context, sanitizedName + ".stddev", convertIfNecessary(timer.stdDev(), recordedUnit, sendUnit), cloudWatchUnit, dimensions);
        }
    }

    /** If recordedUnit doesn't match sendUnit, converts recordedUnit into sendUnit. Otherwise, value is returned unchanged. */
    private static double convertIfNecessary(double value, TimeUnit recordedUnit, TimeUnit sendUnit) {
        if (recordedUnit == sendUnit) {
            return value;
        }
        return sendUnit.convert((long) value, recordedUnit);
    }
}
