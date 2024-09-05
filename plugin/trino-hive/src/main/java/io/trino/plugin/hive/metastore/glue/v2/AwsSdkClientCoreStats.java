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
package io.trino.plugin.hive.metastore.glue.v2;

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import java.util.concurrent.atomic.AtomicLong;

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

    public MetricPublisher newRequestMetricCollector()
    {
        return new AwsSdkClientCoreRequestMetricCollector();
    }

    public static class AwsSdkClientCoreRequestMetricCollector
            implements MetricPublisher
    {
        @Override
        public void publish(MetricCollection metricCollection)
        {
//            TimingInfo timingInfo = request.getAWSRequestMetrics().getTimingInfo();
//
//            Number requestCounts = timingInfo.getCounter(RequestCount.name());
//            if (requestCounts != null) {
//                stats.awsRequestCount.update(requestCounts.longValue());
//            }
//
//            Number retryCounts = timingInfo.getCounter(HttpClientRetryCount.name());
//            if (retryCounts != null) {
//                stats.awsRetryCount.update(retryCounts.longValue());
//            }
//
//            Number throttleExceptions = timingInfo.getCounter(ThrottleException.name());
//            if (throttleExceptions != null) {
//                stats.awsThrottleExceptions.update(throttleExceptions.longValue());
//            }
//
//            Number httpClientPoolAvailableCount = timingInfo.getCounter(HttpClientPoolAvailableCount.name());
//            if (httpClientPoolAvailableCount != null) {
//                stats.awsHttpClientPoolAvailableCount.set(httpClientPoolAvailableCount.longValue());
//            }
//
//            Number httpClientPoolLeasedCount = timingInfo.getCounter(HttpClientPoolLeasedCount.name());
//            if (httpClientPoolLeasedCount != null) {
//                stats.awsHttpClientPoolLeasedCount.set(httpClientPoolLeasedCount.longValue());
//            }
//
//            Number httpClientPoolPendingCount = timingInfo.getCounter(HttpClientPoolPendingCount.name());
//            if (httpClientPoolPendingCount != null) {
//                stats.awsHttpClientPoolPendingCount.set(httpClientPoolPendingCount.longValue());
//            }
//
//            recordSubTimingDurations(timingInfo, HttpRequestTime, stats.awsRequestTime);
//            recordSubTimingDurations(timingInfo, ClientExecuteTime, stats.awsClientExecuteTime);
//            recordSubTimingDurations(timingInfo, RetryPauseTime, stats.awsClientRetryPauseTime);
        }

        @Override
        public void close()
        {
        }

//        private static void recordSubTimingDurations(TimingInfo timingInfo, AWSRequestMetrics.Field field, TimeStat timeStat)
//        {
//            List<TimingInfo> subTimings = timingInfo.getAllSubMeasurements(field.name());
//            if (subTimings != null) {
//                for (TimingInfo subTiming : subTimings) {
//                    Long endTimeNanos = subTiming.getEndTimeNanoIfKnown();
//                    if (endTimeNanos != null) {
//                        timeStat.addNanos(endTimeNanos - subTiming.getStartTimeNano());
//                    }
//                }
//            }
//        }
    }
}
