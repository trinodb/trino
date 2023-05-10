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
package io.trino.hdfs.s3;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.util.AWSRequestMetrics;
import com.amazonaws.util.TimingInfo;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.util.AWSRequestMetrics.Field.ClientExecuteTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpClientPoolAvailableCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpClientPoolLeasedCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpClientPoolPendingCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpClientRetryCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpRequestTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.RetryPauseTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.ThrottleException;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public final class AwsSdkClientCoreStats
{
    private final CounterStat awsRequestCount = new CounterStat();
    private final CounterStat awsRetryCount = new CounterStat();
    private final CounterStat awsThrottleExceptions = new CounterStat();
    private final TimeStat awsRequestTime = new TimeStat(MILLISECONDS);
    private final TimeStat awsClientExecuteTime = new TimeStat(MILLISECONDS);
    private final TimeStat awsClientRetryPauseTime = new TimeStat(MILLISECONDS);
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
    public TimeStat getAwsRequestTime()
    {
        return awsRequestTime;
    }

    @Managed
    @Nested
    public TimeStat getAwsClientExecuteTime()
    {
        return awsClientExecuteTime;
    }

    @Managed
    @Nested
    public TimeStat getAwsClientRetryPauseTime()
    {
        return awsClientRetryPauseTime;
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

    public AwsSdkClientCoreRequestMetricCollector newRequestMetricCollector()
    {
        return new AwsSdkClientCoreRequestMetricCollector(this);
    }

    public static class AwsSdkClientCoreRequestMetricCollector
            extends RequestMetricCollector
    {
        private final AwsSdkClientCoreStats stats;

        protected AwsSdkClientCoreRequestMetricCollector(AwsSdkClientCoreStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public void collectMetrics(Request<?> request, Response<?> response)
        {
            TimingInfo timingInfo = request.getAWSRequestMetrics().getTimingInfo();

            Number requestCounts = timingInfo.getCounter(RequestCount.name());
            if (requestCounts != null) {
                stats.awsRequestCount.update(requestCounts.longValue());
            }

            Number retryCounts = timingInfo.getCounter(HttpClientRetryCount.name());
            if (retryCounts != null) {
                stats.awsRetryCount.update(retryCounts.longValue());
            }

            Number throttleExceptions = timingInfo.getCounter(ThrottleException.name());
            if (throttleExceptions != null) {
                stats.awsThrottleExceptions.update(throttleExceptions.longValue());
            }

            Number httpClientPoolAvailableCount = timingInfo.getCounter(HttpClientPoolAvailableCount.name());
            if (httpClientPoolAvailableCount != null) {
                stats.awsHttpClientPoolAvailableCount.set(httpClientPoolAvailableCount.longValue());
            }

            Number httpClientPoolLeasedCount = timingInfo.getCounter(HttpClientPoolLeasedCount.name());
            if (httpClientPoolLeasedCount != null) {
                stats.awsHttpClientPoolLeasedCount.set(httpClientPoolLeasedCount.longValue());
            }

            Number httpClientPoolPendingCount = timingInfo.getCounter(HttpClientPoolPendingCount.name());
            if (httpClientPoolPendingCount != null) {
                stats.awsHttpClientPoolPendingCount.set(httpClientPoolPendingCount.longValue());
            }

            recordSubTimingDurations(timingInfo, HttpRequestTime, stats.awsRequestTime);
            recordSubTimingDurations(timingInfo, ClientExecuteTime, stats.awsClientExecuteTime);
            recordSubTimingDurations(timingInfo, RetryPauseTime, stats.awsClientRetryPauseTime);
        }

        private static void recordSubTimingDurations(TimingInfo timingInfo, AWSRequestMetrics.Field field, TimeStat timeStat)
        {
            List<TimingInfo> subTimings = timingInfo.getAllSubMeasurements(field.name());
            if (subTimings != null) {
                for (TimingInfo subTiming : subTimings) {
                    Long endTimeNanos = subTiming.getEndTimeNanoIfKnown();
                    if (endTimeNanos != null) {
                        timeStat.addNanos(endTimeNanos - subTiming.getStartTimeNano());
                    }
                }
            }
        }
    }
}
