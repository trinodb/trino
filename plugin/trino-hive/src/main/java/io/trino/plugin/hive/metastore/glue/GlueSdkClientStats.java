/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.glue;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import software.amazon.awssdk.core.internal.metrics.SdkErrorType;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GlueSdkClientStats
{
    private final CounterStat awsRequestCount = new CounterStat();
    private final CounterStat awsRetryCount = new CounterStat();
    private final CounterStat awsThrottleExceptions = new CounterStat();
    private final TimeStat awsServiceCallDuration = new TimeStat(MILLISECONDS);
    private final TimeStat awsApiCallDuration = new TimeStat(MILLISECONDS);
    private final TimeStat awsBackoffDelayDuration = new TimeStat(MILLISECONDS);
    private final AtomicLong awsHttpClientPoolAvailableCount = new AtomicLong();
    private final AtomicLong awsHttpClientPoolLeasedCount = new AtomicLong();
    private final AtomicLong awsHttpClientPoolPendingCount = new AtomicLong();

    @Managed
    @Nested
    public CounterStat getAwsRequestCount()
    {
        return awsRequestCount;
    }

    @Managed
    @Nested
    public CounterStat getAwsRetryCount()
    {
        return awsRetryCount;
    }

    @Managed
    @Nested
    public CounterStat getAwsThrottleExceptions()
    {
        return awsThrottleExceptions;
    }

    @Managed
    @Nested
    public TimeStat getAwsServiceCallDuration()
    {
        return awsServiceCallDuration;
    }

    @Managed
    @Nested
    public TimeStat getAwsApiCallDuration()
    {
        return awsApiCallDuration;
    }

    @Managed
    @Nested
    public TimeStat getAwsBackoffDelayDuration()
    {
        return awsBackoffDelayDuration;
    }

    @Managed
    public long getAwsHttpClientPoolAvailableCount()
    {
        return awsHttpClientPoolAvailableCount.get();
    }

    @Managed
    public long getAwsHttpClientPoolLeasedCount()
    {
        return awsHttpClientPoolLeasedCount.get();
    }

    @Managed
    public long getAwsHttpClientPoolPendingCount()
    {
        return awsHttpClientPoolPendingCount.get();
    }

    public GlueSdkClientRequestMetricsPublisher newRequestMetricsPublisher()
    {
        return new GlueSdkClientRequestMetricsPublisher(this);
    }

    public static class GlueSdkClientRequestMetricsPublisher
            implements MetricPublisher
    {
        private final GlueSdkClientStats stats;

        protected GlueSdkClientRequestMetricsPublisher(GlueSdkClientStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public void publish(MetricCollection metricCollection)
        {
            var requestCount = metricCollection.metricValues(CoreMetric.RETRY_COUNT)
                    .stream()
                    .map(i -> i + 1)
                    .reduce(Integer::sum).orElse(0);
            stats.awsRequestCount.update(requestCount);

            var retryCount = metricCollection.metricValues(CoreMetric.RETRY_COUNT)
                    .stream()
                    .reduce(Integer::sum).orElse(0);
            stats.awsRetryCount.update(retryCount);

            var throttleExceptions = metricCollection
                    .childrenWithName("ApiCallAttempt")
                    .flatMap(mc -> mc.metricValues(CoreMetric.ERROR_TYPE).stream())
                    .filter(s -> s.equals(SdkErrorType.THROTTLING.toString()))
                    .count();
            stats.awsThrottleExceptions.update(throttleExceptions);

            var serviceCallDuration = metricCollection
                    .childrenWithName("ApiCallAttempt")
                    .flatMap(mc -> mc.metricValues(CoreMetric.SERVICE_CALL_DURATION).stream())
                    .reduce(Duration::plus).orElse(Duration.ZERO);
            stats.awsServiceCallDuration.add(serviceCallDuration.toMillis(), MILLISECONDS);

            var apiCallDuration = metricCollection
                    .metricValues(CoreMetric.API_CALL_DURATION)
                    .stream().reduce(Duration::plus).orElse(Duration.ZERO);
            stats.awsApiCallDuration.add(apiCallDuration.toMillis(), MILLISECONDS);

            var backoffDelayDuration = metricCollection
                    .childrenWithName("ApiCallAttempt")
                    .flatMap(mc -> mc.metricValues(CoreMetric.BACKOFF_DELAY_DURATION).stream())
                    .reduce(Duration::plus).orElse(Duration.ZERO);
            stats.awsBackoffDelayDuration.add(backoffDelayDuration.toMillis(), MILLISECONDS);

            var httpClientPoolAvailableCount = metricCollection.childrenWithName("ApiCallAttempt")
                    .flatMap(attempt -> attempt.childrenWithName("HttpClient"))
                    .flatMap(httpClient -> httpClient.metricValues(HttpMetric.AVAILABLE_CONCURRENCY).stream())
                    .reduce(Integer::max).orElse(0);
            stats.awsHttpClientPoolAvailableCount.set(httpClientPoolAvailableCount);

            var httpClientPoolLeasedCount = metricCollection.childrenWithName("ApiCallAttempt")
                    .flatMap(attempt -> attempt.childrenWithName("HttpClient"))
                    .flatMap(httpClient -> httpClient.metricValues(HttpMetric.LEASED_CONCURRENCY).stream())
                    .reduce(Integer::max).orElse(0);
            stats.awsHttpClientPoolLeasedCount.set(httpClientPoolLeasedCount);

            var httpClientPoolPendingCount = metricCollection.childrenWithName("ApiCallAttempt")
                    .flatMap(attempt -> attempt.childrenWithName("HttpClient"))
                    .flatMap(httpClient -> httpClient.metricValues(HttpMetric.PENDING_CONCURRENCY_ACQUIRES).stream())
                    .reduce(Integer::max).orElse(0);
            stats.awsHttpClientPoolPendingCount.set(httpClientPoolPendingCount);
        }

        @Override
        public void close()
        {
        }
    }
}
