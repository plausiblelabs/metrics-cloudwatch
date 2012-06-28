package com.plausiblelabs.metrics.reporting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class CloudWatchReporter extends AbstractPollingReporter implements MetricProcessor<Date> {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class);

    /**
     * CloudWatch metrics are 50 cents per unique custom metric per month. Setting this to true excludes less valuable
     * information like the total number of values seen by a meter.
     */
    private static final boolean USE_FEW_CLOUD_WATCH_METRICS = true;
    
    private final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
    private final List<Dimension> instanceDimensions = new ArrayList<Dimension>();
    private final Set<MetricName> unsendable = new HashSet<MetricName>();
    private final Set<MetricName> nonCloudWatchUnit = new HashSet<MetricName>();
    private final String namespace;
    private final AmazonCloudWatchClient client;
    private final MetricPredicate predicate;

    private boolean attemptedFetchingInstanceId;
    private String instanceId;


    private PutMetricDataRequest putReq;

    public CloudWatchReporter(MetricsRegistry registry, String namespace, AWSCredentials creds, MetricPredicate predicate) {
        super(registry, "cloudwatch-reporter");
        this.predicate = predicate;

        this.namespace = namespace;
        client = new AmazonCloudWatchClient(creds);
    }

    /**
     * Sets the InstanceId dimension sent along with the CloudWatch metrics. This will be found automatically if run
     * on EC2. If run outside EC2, this must be called or no metrics will be sent.
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        instanceDimensions.add(new Dimension().withName("InstanceId").withValue(instanceId));
    }

    @Override
    public void run() {
        if (instanceId == null) {
            fetchInstanceId();
            if (instanceId == null) {
                return;
            }
        }

        putReq = new PutMetricDataRequest().withNamespace(namespace);
        try {
            Date timestamp = new Date();
            sendVMMetrics(timestamp);
            sendRegularMetrics(timestamp);
            if (!putReq.getMetricData().isEmpty()) {
                client.putMetricData(putReq);
            }
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
                    LOG.warn("Got bad response code {} fetching instanceId; will retry on next scheduled report. No metrics will be reported until it succeeds. If running outside EC2, call setInstanceId on the CloudWatchReporter.", resp.getStatusLine().getStatusCode());
                }
                return;
            }
            InputStream content = resp.getEntity().getContent();
            setInstanceId(new BufferedReader(new InputStreamReader(content, "ASCII")).readLine());
            if (attemptedFetchingInstanceId) {
                LOG.warn("Succeeded fetching instanceId after failure; all new metrics will now be reported");
            }
        } catch (Exception e) {
            if (!attemptedFetchingInstanceId) {
                LOG.warn("Failed fetching instanceId; will retry on next scheduled report. No metrics will be reported until it succeeds. If running outside EC2, call setInstanceId on the CloudWatchReporter.", e);
            }
        } finally {
            attemptedFetchingInstanceId = true;
            httpClient.getConnectionManager().shutdown();
        }
    }

    protected void sendRegularMetrics(Date timestamp) {
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


    protected void sendVMMetrics(Date timestamp) {
        sendValue(timestamp, "jvm.memory.heap_usage", vm.heapUsage(), StandardUnit.Percent);
        sendValue(timestamp, "jvm.thread_count", vm.threadCount(), StandardUnit.Count);
        sendValue(timestamp, "jvm.fd_usage", vm.fileDescriptorUsage(), StandardUnit.Percent);

        if (USE_FEW_CLOUD_WATCH_METRICS) {
            return;
        }
        sendValue(timestamp, "jvm.memory.non_heap_usage", vm.nonHeapUsage(), StandardUnit.Percent);

        sendValue(timestamp, "jvm.daemon_thread_count", vm.daemonThreadCount(), StandardUnit.Count);

        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
            sendValue(timestamp, "jvm.thread-states." + entry.getKey().toString().toLowerCase(), entry.getValue(), StandardUnit.Count);
        }

        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            sendValue(timestamp, "jvm.gc." + entry.getKey() + ".time", entry.getValue().getTime(TimeUnit.MILLISECONDS), StandardUnit.Milliseconds);
            sendValue(timestamp, "jvm.gc." + entry.getKey() + ".runs", entry.getValue().getRuns(), StandardUnit.Count);
        }
    }

    protected void sendValue(Date timestamp, String name, double value, StandardUnit unit, Dimension...additionalDimensions) {
        MetricDatum datum = new MetricDatum()
            .withTimestamp(timestamp)
            .withValue(value)
            .withMetricName(name)
            .withDimensions(instanceDimensions)
            .withDimensions(additionalDimensions)
            .withUnit(unit);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending {}", datum);
        }
        putReq.withMetricData(datum);

        // Can only send 20 metrics at a time
        if (putReq.getMetricData().size() == 20) {
            client.putMetricData(putReq);
            putReq = new PutMetricDataRequest().withNamespace(namespace);
        }
    }

    protected String sanitizeName(MetricName name) {
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
            sendValue(context, sanitizeName(name), ((Number) gauge.value()).doubleValue(), StandardUnit.None);
        } else if(unsendable.add(name)) {
            LOG.warn("The type of the value for {} is {}. It must be a subclass of Number to send to CloudWatch.", name, gauge.value().getClass());
        }
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Date context) throws Exception {
        sendValue(context, sanitizeName(name), counter.count(), StandardUnit.Count);
    }


    @Override
    public void processMeter(MetricName name, Metered meter, Date context) throws Exception {
        String sanitizedName = sanitizeName(name);
        String rateUnits = meter.rateUnit().name();
        String rateUnit = rateUnits.substring(0, rateUnits.length() - 1).toLowerCase(Locale.US);
        // CloudWatch only supports its standard units, so this rate won't line up. Instead send the unit as a dimension and call the unit Counts.
        Dimension unit = new Dimension().withName("meterUnit").withValue(meter.eventType() + '/' + rateUnit);
        sendValue(context, sanitizedName + ".1MinuteRate", meter.oneMinuteRate(), StandardUnit.None, unit);
        if (USE_FEW_CLOUD_WATCH_METRICS) {
            return;
        }
        sendValue(context, sanitizedName + ".count", meter.count(), StandardUnit.None, unit);
        sendValue(context, sanitizedName + ".meanRate", meter.meanRate(), StandardUnit.None, unit);
        sendValue(context, sanitizedName + ".5MinuteRate", meter.fiveMinuteRate(), StandardUnit.None, unit);
        sendValue(context, sanitizedName + ".15MinuteRate", meter.fifteenMinuteRate(), StandardUnit.None, unit);
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Date context) throws Exception {
        String sanitizedName = sanitizeName(name);
        Snapshot snapshot = histogram.getSnapshot();
        sendValue(context, sanitizedName + ".median", snapshot.getMedian(), StandardUnit.None);
        sendValue(context, sanitizedName + ".95percentile", snapshot.get95thPercentile(), StandardUnit.None);
        sendValue(context, sanitizedName + ".99percentile", snapshot.get99thPercentile(), StandardUnit.None);
        if (USE_FEW_CLOUD_WATCH_METRICS) {
            return;
        }
        sendValue(context, sanitizedName + ".75percentile", snapshot.get75thPercentile(), StandardUnit.None);
        sendValue(context, sanitizedName + ".98percentile", snapshot.get98thPercentile(), StandardUnit.None);
        sendValue(context, sanitizedName + ".999percentile", snapshot.get999thPercentile(), StandardUnit.None);
        sendValue(context, sanitizedName + ".min", histogram.min(), StandardUnit.None);
        sendValue(context, sanitizedName + ".max", histogram.max(), StandardUnit.None);
        sendValue(context, sanitizedName + ".mean", histogram.mean(), StandardUnit.None);
        sendValue(context, sanitizedName + ".stddev", histogram.stdDev(), StandardUnit.None);
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
                LOG.debug("Cloud Watch doesn't support nanosecond units; converting {} to seconds.", name);
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
        String sanitizedName = sanitizeName(name);
        Snapshot snapshot = timer.getSnapshot();
        sendValue(context, sanitizedName + ".median", convertIfNecessary(snapshot.getMedian(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".95percentile", convertIfNecessary(snapshot.get95thPercentile(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".99percentile", convertIfNecessary(snapshot.get99thPercentile(), recordedUnit, sendUnit), cloudWatchUnit);
        if (USE_FEW_CLOUD_WATCH_METRICS) {
            return;
        }
        sendValue(context, sanitizedName + ".min", convertIfNecessary(timer.min(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".max", convertIfNecessary(timer.max(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".mean", convertIfNecessary(timer.mean(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".stddev", convertIfNecessary(timer.stdDev(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".75percentile", convertIfNecessary(snapshot.get75thPercentile(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".98percentile", convertIfNecessary(snapshot.get98thPercentile(), recordedUnit, sendUnit), cloudWatchUnit);
        sendValue(context, sanitizedName + ".999percentile", convertIfNecessary(snapshot.get999thPercentile(), recordedUnit, sendUnit), cloudWatchUnit);
    }

    /** If recordedUnit doesn't match sendUnit, converts recordedUnit into sendUnit. Otherwise, value is returned unchanged. */
    private static double convertIfNecessary(double value, TimeUnit recordedUnit, TimeUnit sendUnit) {
        if (recordedUnit == sendUnit) {
            return value;
        }
        return sendUnit.convert((long) value, recordedUnit);
    }
}
