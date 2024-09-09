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
package io.trino.filesystem.s3;

import io.airlift.log.Logger;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.SERVER_ERROR;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.THROTTLING;
import static software.amazon.awssdk.core.metrics.CoreMetric.API_CALL_DURATION;
import static software.amazon.awssdk.core.metrics.CoreMetric.API_CALL_SUCCESSFUL;
import static software.amazon.awssdk.core.metrics.CoreMetric.ERROR_TYPE;
import static software.amazon.awssdk.core.metrics.CoreMetric.OPERATION_NAME;
import static software.amazon.awssdk.core.metrics.CoreMetric.RETRY_COUNT;
import static software.amazon.awssdk.core.metrics.CoreMetric.SERVICE_ID;

public class S3FileSystemStats
{
    private final AwsSdkV2ApiCallStats total = new AwsSdkV2ApiCallStats();

    private final AwsSdkV2ApiCallStats headObject = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats getObject = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats listObjectsV2 = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats putObject = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats deleteObject = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats deleteObjects = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats createMultipartUpload = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats completeMultipartUpload = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats abortMultipartUpload = new AwsSdkV2ApiCallStats();
    private final AwsSdkV2ApiCallStats uploadPart = new AwsSdkV2ApiCallStats();

    private static final AwsSdkV2ApiCallStats dummy = new DummyAwsSdkV2ApiCallStats();

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getTotal()
    {
        return total;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getHeadObject()
    {
        return headObject;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getGetObject()
    {
        return getObject;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getListObjectsV2()
    {
        return listObjectsV2;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getPutObject()
    {
        return putObject;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getDeleteObject()
    {
        return deleteObject;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getDeleteObjects()
    {
        return deleteObjects;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getCreateMultipartUpload()
    {
        return createMultipartUpload;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getCompleteMultipartUpload()
    {
        return completeMultipartUpload;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats getAbortMultipartUpload()
    {
        return abortMultipartUpload;
    }

    @Managed
    @Nested
    public AwsSdkV2ApiCallStats uploadPart()
    {
        return uploadPart;
    }

    public MetricPublisher newMetricPublisher()
    {
        return new JmxMetricPublisher(this);
    }

    public static final class JmxMetricPublisher
            implements MetricPublisher
    {
        private static final Set<SdkMetric<?>> ALLOWED_METRICS = Set.of(API_CALL_SUCCESSFUL, RETRY_COUNT, API_CALL_DURATION, ERROR_TYPE);

        private static final Logger log = Logger.get(JmxMetricPublisher.class);

        private final S3FileSystemStats stats;

        public JmxMetricPublisher(S3FileSystemStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public void publish(MetricCollection metricCollection)
        {
            try {
                Optional<String> serviceId = metricCollection.metricValues(SERVICE_ID).stream().filter(Objects::nonNull).findFirst();
                Optional<String> operationName = metricCollection.metricValues(OPERATION_NAME).stream().filter(Objects::nonNull).findFirst();
                if (serviceId.isEmpty() || operationName.isEmpty()) {
                    log.warn("ServiceId or OperationName is empty for AWS MetricCollection: %s", metricCollection);
                    return;
                }

                if (!serviceId.get().equals("S3")) {
                    return;
                }

                AwsSdkV2ApiCallStats apiCallStats = getApiCallStats(operationName.get());
                publishMetrics(metricCollection, apiCallStats);
            }
            catch (Exception e) {
                log.warn(e, "Publishing AWS metrics failed");
            }
        }

        private void publishMetrics(MetricCollection metricCollection, AwsSdkV2ApiCallStats apiCallStats)
        {
            metricCollection.stream()
                    .filter(metricRecord -> metricRecord.value() != null && ALLOWED_METRICS.contains(metricRecord.metric()))
                    .forEach(metricRecord -> {
                        if (metricRecord.metric().equals(API_CALL_SUCCESSFUL)) {
                            Boolean value = (Boolean) metricRecord.value();

                            stats.total.updateCalls();
                            apiCallStats.updateCalls();

                            if (value.equals(Boolean.FALSE)) {
                                stats.total.updateFailures();
                                apiCallStats.updateFailures();
                            }
                        }
                        else if (metricRecord.metric().equals(RETRY_COUNT)) {
                            int value = (int) metricRecord.value();

                            stats.total.updateRetries(value);
                            apiCallStats.updateRetries(value);
                        }
                        else if (metricRecord.metric().equals(API_CALL_DURATION)) {
                            Duration value = (Duration) metricRecord.value();

                            stats.total.updateLatency(value);
                            apiCallStats.updateLatency(value);
                        }
                        else if (metricRecord.metric().equals(ERROR_TYPE)) {
                            String value = (String) metricRecord.value();

                            if (value.equals(THROTTLING.toString())) {
                                stats.total.updateThrottlingExceptions();
                                apiCallStats.updateThrottlingExceptions();
                            }
                            else if (value.equals(SERVER_ERROR.toString())) {
                                stats.total.updateServerErrors();
                                apiCallStats.updateServerErrors();
                            }
                        }
                    });

            metricCollection.children().forEach(child -> publishMetrics(child, apiCallStats));
        }

        @Override
        public void close()
        {
        }

        private AwsSdkV2ApiCallStats getApiCallStats(String operationName)
        {
            return switch (operationName) {
                case "HeadObject" -> stats.headObject;
                case "GetObject" -> stats.getObject;
                case "ListObjectsV2" -> stats.listObjectsV2;
                case "PutObject" -> stats.putObject;
                case "DeleteObject" -> stats.deleteObject;
                case "DeleteObjects" -> stats.deleteObjects;
                case "CreateMultipartUpload" -> stats.createMultipartUpload;
                case "CompleteMultipartUpload" -> stats.completeMultipartUpload;
                case "AbortMultipartUpload" -> stats.abortMultipartUpload;
                case "UploadPart" -> stats.uploadPart;
                default -> S3FileSystemStats.dummy;
            };
        }
    }

    private static class DummyAwsSdkV2ApiCallStats
            extends AwsSdkV2ApiCallStats
    {
        @Override
        public void updateLatency(Duration duration) {}

        @Override
        public void updateCalls() {}

        @Override
        public void updateFailures() {}

        @Override
        public void updateRetries(int retryCount) {}

        @Override
        public void updateThrottlingExceptions() {}

        @Override
        public void updateServerErrors() {}
    }
}
