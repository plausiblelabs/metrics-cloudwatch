package com.plausiblelabs.metrics.reporting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Reports metrics to <a href="http://aws.amazon.com/cloudwatch/">Amazon's CloudWatch</a> periodically.
 */
public class CloudWatchReporter extends ScheduledReporter {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class);

    /**
     * Amazon's docs say they don't accept values smaller than 1E-130, but experimentally 1E-108 is the smallest
     * accepted value. Metric values smaller than this will be trimmed to this value and a debug log will be printed the
     * first time it happens in the reporter.
     */
    static final double SMALLEST_SENDABLE = 1E-108;

    /**
     * Amazon's docs say they don't accept values larger than 1E116, but experimentally 1E108 is the smallest
     * accepted value. Metric values larger than this will be trimmed to this value and a debug log will be printed the
     * first time it happens in the reporter.
     */
    static final double LARGEST_SENDABLE = 1E108;
    private final StandardUnit cloudWatchUnit;

    /**
     * <p>Creates or starts a CloudWatchReporter.</p>
     * <p>As CloudWatch charges 50 cents per unique metric, this reporter attempts to be parsimonious with the values
     * it sends by default. It only sends the median, 95th, and 99th percentiles for histograms and timers, and the
     * one minute rate for meters and timers. Additional metrics may be sent through configuring this class.</p>
     */
    public static class Builder {

        private final String namespace;
        private final AmazonCloudWatchClient client;
        private final List<DimensionAdder> dimensionAdders = new ArrayList<DimensionAdder>();
        private final MetricRegistry registry;

        private TimeUnit durationUnit;
        private TimeUnit rateUnit;

        private MetricFilter filter = MetricFilter.ALL;

        private double[] percentilesToSend = {.5, .95, .99};
        private boolean sendOneMinute = true, sendFiveMinute, sendFifteenMinute;
        private boolean sendMeterSummary;
        private boolean sendTimerLifetime;
        private boolean sendHistoLifetime;

        /**
         * Creates an Builder that sends values in the given namespace to the given AWS account
         *
         * @param namespace the namespace. Must be non-null and not empty.
         */
        public Builder(MetricRegistry registry, String namespace, AWSCredentials creds) {
            this(registry, namespace, new AmazonCloudWatchClient(creds));
        }

        /**
         * Creates an Builder that sends values in the given namespace using the given client
         *
         * @param namespace the namespace. Must be non-null and not empty.
         */
        public Builder(MetricRegistry registry, String namespace, AmazonCloudWatchClient client) {
            this.registry = registry;
            this.namespace = namespace;
            this.client = client;

            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;

        }

        /**
         * <p>The histogram and meter percentiles to send. If <code>.5</code> is included, it'll be reported as
         * <code>median</code>.This defaults to <code>.5, .95, and .99</code>.
         *
         * @param percentiles the percentiles to send. Replaces the currently set percentiles.
         * @return this Builder.
         */
        public Builder withPercentiles(double... percentiles) {
            this.percentilesToSend = percentiles;
            return this;
        }

        /**
         * If the one minute rate should be sent for meters and timers. Enabled by default.
         *
         * @param enabled if the rate should be sent.
         * @return this Builder.
         */
        public Builder withOneMinuteRate(boolean enabled) {
            this.sendOneMinute = enabled;
            return this;
        }


        /**
         * If the five minute rate should be sent for meters and timers. Disabled by default.
         *
         * @param enabled if the rate should be sent.
         * @return this Builder.
         */
        public Builder withFiveMinuteRate(boolean enabled) {
            this.sendFiveMinute = enabled;
            return this;
        }

        /**
         * If the fifteen minute rate should be sent for meters and timers. Disabled by default.
         *
         * @param enabled if the rate should be sent.
         * @return this Builder.
         */
        public Builder withFifteenMinuteRate(boolean enabled) {
            this.sendFifteenMinute = enabled;
            return this;
        }

        /**
         * If the lifetime value summary should be sent for meters ie count and mean rate. Disabled by default.
         *
         * @param enabled if the summary should be sent.
         * @return this Builder.
         */
        public Builder withMeterSummary(boolean enabled) {
            this.sendMeterSummary = enabled;
            return this;
        }

        /**
         * If the lifetime value summary should be sent for timers ie min, max, mean, and count. Disabled by default.
         *
         * @param enabled if the summary should be sent.
         * @return this Builder.
         */
        public Builder withTimerSummary(boolean enabled) {
            this.sendTimerLifetime = enabled;
            return this;
        }

        /**
         * If the lifetime value summary should be sent for histograms ie min, max, mean, and count. Disabled by default.
         *
         * @param enabled if the summary should be sent.
         * @return this Builder.
         */
        public Builder withHistogramSummary(boolean enabled) {
            this.sendHistoLifetime = enabled;
            return this;
        }


        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * <p>Adds an <code>InstanceId</code> dimension to all sent metrics with EC2 instance's id. The id isfetched
         * from the EC2 metadata server at <code>http://169.254.169.254/latest/meta-data/instance-id</code>.</p>
         * <p/>
         * <p>This will only work if running inside EC2. If used outside of EC2, or if the service fails, an
         * <code>InstanceId</code> dimenson with the value <code>unknown</code> will be sent.</p>
         *
         * @return this Builder.
         */
        public Builder withEC2InstanceIdDimension() {
            return withEC2InstanceIdDimension(MetricFilter.ALL);
        }

        /**
         * <p>Adds an <code>InstanceId</code> dimension to all metrics matching the given predicate. The instance id
         * fetched from the EC2 metadata server at <code>http://169.254.169.254/latest/meta-data/instance-id</code>.</p>
         * <p/>
         * <p>This will only work if running inside EC2. If used outside of EC2, or if the service fails, an
         * <code>InstanceId</code> dimenson with the value <code>unknown</code> will be sent.</p>
         *
         * @return this Builder.
         */
        public Builder withEC2InstanceIdDimension(MetricFilter predicate) {
            return withDimensionAdder(new InstanceIdAdder(predicate));
        }

        /**
         * <p>Adds an <code>InstanceId</code> dimension to all metrics with the given instance id as a value.</p>
         *
         * @return this Builder.
         */
        public Builder withInstanceIdDimension(String instanceId) {
            return withInstanceIdDimension(instanceId, MetricFilter.ALL);
        }

        /**
         * <p>Adds an <code>InstanceId</code> dimension to all metrics matching the given predicate with the given
         * instance id as a value.</p>
         *
         * @return this Builder.
         */
        public Builder withInstanceIdDimension(String instanceId, MetricFilter predicate) {
            return withDimensionAdder(new InstanceIdAdder(predicate, instanceId));
        }

        /**
         * Runs the given adder on all sent metrics. Note: a single metric may have a maximum of 10 dimensions.
         *
         * @return this Builder.
         */
        public Builder withDimensionAdder(DimensionAdder adder) {
            this.dimensionAdders.add(adder);
            return this;
        }

        /**
         * Creates a reporter with the settings currently configured on this enabler.
         */
        public CloudWatchReporter build() {
            return new CloudWatchReporter(registry, namespace, client, filter, rateUnit, durationUnit,
                    dimensionAdders,
                    percentilesToSend, sendOneMinute, sendFiveMinute,
                    sendFifteenMinute, sendMeterSummary, sendTimerLifetime, sendHistoLifetime);
        }

    }

    private final List<DimensionAdder> dimensionAdders;
    private final Set<String> unsendable = new HashSet<String>();
    private final Set<String> nonCloudWatchUnit = new HashSet<String>();
    private final String namespace;
    private final AmazonCloudWatchClient client;

    private final double[] percentilesToSend;
    private final boolean sendOneMinute, sendFiveMinute, sendFifteenMinute;
    private final boolean sendMeterSummary;
    private final boolean sendTimerLifetime;
    private final boolean sendHistoLifetime;
    private final Clock clock;

    private PutMetricDataRequest putReq;

    private CloudWatchReporter(MetricRegistry registry, String namespace, AmazonCloudWatchClient client,
                               MetricFilter predicate, TimeUnit rateUnit, TimeUnit durationUnit, List<DimensionAdder> dimensionAdders,
                               double[] percentilesToSend, boolean sendOneMinute,
                               boolean sendFiveMinute, boolean sendFifteenMinute, boolean sendMeterSummary,
                               boolean sendTimerLifetime, boolean sendHistoLifetime) {

        super(registry, "cloudwatch-reporter", predicate, rateUnit, durationUnit);

        this.clock = Clock.defaultClock();

        switch (durationUnit) {
        case SECONDS:
            cloudWatchUnit = StandardUnit.Seconds;
            break;
        case MILLISECONDS:
            cloudWatchUnit = StandardUnit.Milliseconds;
            break;
        case MICROSECONDS:
            cloudWatchUnit = StandardUnit.Microseconds;
            break;
        default:
            throw new IllegalArgumentException("unsupported durationUnit: "+durationUnit);
        }

        this.namespace = namespace;
        this.client = client;
        this.dimensionAdders = dimensionAdders;

        this.percentilesToSend = percentilesToSend;
        this.sendOneMinute = sendOneMinute;
        this.sendFiveMinute = sendFiveMinute;
        this.sendFifteenMinute = sendFifteenMinute;
        this.sendMeterSummary = sendMeterSummary;
        this.sendTimerLifetime = sendTimerLifetime;
        this.sendHistoLifetime = sendHistoLifetime;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

        putReq = new PutMetricDataRequest().withNamespace(namespace);

        final Date context = new Date(clock.getTime());
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            processGauge(entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            processCounter(entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            processHistogram(entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            processMeter(entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            processTimer(entry.getKey(), entry.getValue(), context);
        }

        try {
            sendToCloudWatch();
        } catch (Exception e) {
            LOG.warn("Error writing to CloudWatch", e);
        } finally {
            putReq = null;
        }
    }

    private void sendToCloudWatch() {
        try {
            if (!putReq.getMetricData().isEmpty()) {
                client.putMetricData(putReq);
            }
        } catch (RuntimeException re) {
            LOG.warn("Failed writing to CloudWatch: {}", putReq);
        } finally {
            // Be sure the putReq cleared; a failure indicates bad data, so we don't want to try again
            putReq = new PutMetricDataRequest().withNamespace(namespace);
        }
    }

    private boolean sentTooSmall, sentTooLarge;

    private void sendValue(Date timestamp, String name, double value, StandardUnit unit, List<Dimension> dimensions) {
        double absValue = Math.abs(value);
        if (absValue < SMALLEST_SENDABLE) {
            if (absValue > 0) {// Allow 0 through untouched, everything else gets rounded to SMALLEST_SENDABLE
                if (value < 0) {
                    value = -SMALLEST_SENDABLE;
                } else {
                    value = SMALLEST_SENDABLE;
                }
                if (!sentTooSmall) {
                    LOG.debug("Value for {} is smaller than what CloudWatch supports; trimming to {}. Further small values won't be logged.", name, value);
                    sentTooSmall = true;
                }
            }
        } else if (absValue > LARGEST_SENDABLE) {
            if (value < 0) {
                value = -LARGEST_SENDABLE;
            } else {
                value = LARGEST_SENDABLE;
            }
            if (!sentTooLarge) {
                LOG.debug("Value for {} is larger than what CloudWatch supports; trimming to {}. Further large values won't be logged.", name, value);
                sentTooLarge = true;
            }
        }
        // TODO limit to 10 dimensions
        MetricDatum datum = new MetricDatum()
                .withTimestamp(timestamp)
                .withValue(value)
                .withMetricName(name)
                .withDimensions(dimensions)
                .withUnit(unit);


        LOG.debug("Sending {}", datum);

        putReq.withMetricData(datum);

        // Can only send 20 metrics at a time
        if (putReq.getMetricData().size() == 20) {
            sendToCloudWatch();
        }
    }

    private List<Dimension> createDimensions(String name, Metric metric) {
        List<Dimension> dimensions = new ArrayList<Dimension>();
        for (DimensionAdder adder : dimensionAdders) {
            dimensions.addAll(adder.generate(name, metric));
        }
        return dimensions;
    }

    void processGauge(String name, Gauge<?> gauge, Date context) {

        String sanitizedName = sanitize(name);

        if (gauge.getValue() instanceof Number) {
            sendValue(context, sanitizedName, ((Number) gauge.getValue()).doubleValue(), StandardUnit.None, createDimensions(name, gauge));
        } else if (unsendable.add(name)) {
            LOG.warn("The type of the value for {} is {}. It must be a subclass of Number to send to CloudWatch.", name, gauge.getValue().getClass());
        }
    }

    public void processCounter(String name, Counter counter, Date context) {
        String sanitizedName = sanitize(name);

        sendValue(context, sanitizedName, counter.getCount(), StandardUnit.Count, createDimensions(name, counter));
    }


    void processMeter(String name, Metered meter, Date context) {
        List<Dimension> dimensions = createDimensions(name, meter);
        String sanitizedName = sanitize(name);

        // CloudWatch only supports its standard units, so this rate won't line up. Instead send the unit as a dimension and call the unit None.
//        dimensions.add(new Dimension().withName("meterUnit").withValue(getRateUnit()));
        if (sendOneMinute) {
            sendValue(context, sanitizedName + ".1MinuteRate", meter.getOneMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendFiveMinute) {
            sendValue(context, sanitizedName + ".5MinuteRate", meter.getFiveMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendFifteenMinute) {
            sendValue(context, sanitizedName + ".15MinuteRate", meter.getFifteenMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendMeterSummary) {
            sendValue(context, sanitizedName + ".count", meter.getCount(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".meanRate", meter.getMeanRate(), StandardUnit.None, dimensions);
        }
    }

    void processHistogram(String name, Histogram histogram, Date context) {
        List<Dimension> dimensions = createDimensions(name, histogram);
        String sanitizedName = sanitize(name);
        Snapshot snapshot = histogram.getSnapshot();
        for (double percentile : percentilesToSend) {
            if (percentile == .5) {
                sendValue(context, sanitizedName + ".median", snapshot.getMedian(), StandardUnit.None, dimensions);
            } else {
                sendValue(context, sanitizedName + "_percentile_" + percentile, snapshot.getValue(percentile), StandardUnit.None, dimensions);
            }
        }
        if (sendHistoLifetime) {
            sendValue(context, sanitizedName + ".min", snapshot.getMin(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".max", snapshot.getMax(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".mean", snapshot.getMean(), StandardUnit.None, dimensions);
            sendValue(context, sanitizedName + ".stddev", snapshot.getStdDev(), StandardUnit.None, dimensions);
        }
    }

    public void processTimer(String name, Timer timer, Date context) {

        processMeter(name, timer, context);

        List<Dimension> dimensions = createDimensions(name, timer);
        String sanitizedName = sanitize(name);
        Snapshot snapshot = timer.getSnapshot();

        for (double percentile : percentilesToSend) {
            if (percentile == .5) {
                sendValue(context, sanitizedName + ".median", convertDuration(snapshot.getMedian()), cloudWatchUnit, dimensions);
            } else {
                sendValue(context, sanitizedName + "_percentile_" + percentile, convertDuration(snapshot.getValue(percentile)), cloudWatchUnit, dimensions);
            }
        }
        if (sendTimerLifetime) {
            sendValue(context, sanitizedName + ".min", convertDuration(snapshot.getMin()), cloudWatchUnit, dimensions);
            sendValue(context, sanitizedName + ".max", convertDuration(snapshot.getMax()), cloudWatchUnit, dimensions);
            sendValue(context, sanitizedName + ".mean", convertDuration(snapshot.getMean()), cloudWatchUnit, dimensions);
            sendValue(context, sanitizedName + ".stddev", convertDuration(snapshot.getStdDev()), cloudWatchUnit, dimensions);
        }
    }

    protected String sanitize(String name) {
        return name;
    }
}
