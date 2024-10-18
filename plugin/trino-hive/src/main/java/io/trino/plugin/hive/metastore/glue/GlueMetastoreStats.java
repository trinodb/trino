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

public class GlueMetastoreStats
{
    private final AwsApiCallStats total = new AwsApiCallStats();
    private final AwsApiCallStats dummy = new DummyAwsApiCallStats();

    private final AwsApiCallStats getDatabases = new AwsApiCallStats();
    private final AwsApiCallStats getDatabase = new AwsApiCallStats();
    private final AwsApiCallStats getTables = new AwsApiCallStats();
    private final AwsApiCallStats getTable = new AwsApiCallStats();
    private final AwsApiCallStats createDatabase = new AwsApiCallStats();
    private final AwsApiCallStats deleteDatabase = new AwsApiCallStats();
    private final AwsApiCallStats updateDatabase = new AwsApiCallStats();
    private final AwsApiCallStats createTable = new AwsApiCallStats();
    private final AwsApiCallStats deleteTable = new AwsApiCallStats();
    private final AwsApiCallStats updateTable = new AwsApiCallStats();
    private final AwsApiCallStats getPartitionNames = new AwsApiCallStats();
    private final AwsApiCallStats getPartitions = new AwsApiCallStats();
    private final AwsApiCallStats getPartition = new AwsApiCallStats();
    private final AwsApiCallStats createPartitions = new AwsApiCallStats();
    private final AwsApiCallStats deletePartition = new AwsApiCallStats();
    private final AwsApiCallStats updatePartition = new AwsApiCallStats();
    private final AwsApiCallStats batchUpdatePartition = new AwsApiCallStats();
    private final AwsApiCallStats batchCreatePartition = new AwsApiCallStats();
    private final AwsApiCallStats getColumnStatisticsForTable = new AwsApiCallStats();
    private final AwsApiCallStats getColumnStatisticsForPartition = new AwsApiCallStats();
    private final AwsApiCallStats updateColumnStatisticsForTable = new AwsApiCallStats();
    private final AwsApiCallStats deleteColumnStatisticsForTable = new AwsApiCallStats();
    private final AwsApiCallStats updateColumnStatisticsForPartition = new AwsApiCallStats();
    private final AwsApiCallStats deleteColumnStatisticsForPartition = new AwsApiCallStats();
    private final AwsApiCallStats getUserDefinedFunction = new AwsApiCallStats();
    private final AwsApiCallStats getUserDefinedFunctions = new AwsApiCallStats();
    private final AwsApiCallStats createUserDefinedFunction = new AwsApiCallStats();
    private final AwsApiCallStats updateUserDefinedFunction = new AwsApiCallStats();
    private final AwsApiCallStats deleteUserDefinedFunction = new AwsApiCallStats();

    @Managed
    @Nested
    public AwsApiCallStats getGetDatabases()
    {
        return getDatabases;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetTables()
    {
        return getTables;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public AwsApiCallStats getCreateDatabase()
    {
        return createDatabase;
    }

    @Managed
    @Nested
    public AwsApiCallStats getDeleteDatabase()
    {
        return deleteDatabase;
    }

    @Managed
    @Nested
    public AwsApiCallStats getUpdateDatabase()
    {
        return updateDatabase;
    }

    @Managed
    @Nested
    public AwsApiCallStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public AwsApiCallStats getDeleteTable()
    {
        return deleteTable;
    }

    @Managed
    @Nested
    public AwsApiCallStats getUpdateTable()
    {
        return updateTable;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetPartitionNames()
    {
        return getPartitionNames;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetPartitions()
    {
        return getPartitions;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetPartition()
    {
        return getPartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getCreatePartitions()
    {
        return createPartitions;
    }

    @Managed
    @Nested
    public AwsApiCallStats getDeletePartition()
    {
        return deletePartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getUpdatePartition()
    {
        return updatePartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getBatchUpdatePartition()
    {
        return batchUpdatePartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getBatchCreatePartition()
    {
        return batchCreatePartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetColumnStatisticsForTable()
    {
        return getColumnStatisticsForTable;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetColumnStatisticsForPartition()
    {
        return getColumnStatisticsForPartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getUpdateColumnStatisticsForTable()
    {
        return updateColumnStatisticsForTable;
    }

    @Managed
    @Nested
    public AwsApiCallStats getDeleteColumnStatisticsForTable()
    {
        return deleteColumnStatisticsForTable;
    }

    @Managed
    @Nested
    public AwsApiCallStats getUpdateColumnStatisticsForPartition()
    {
        return updateColumnStatisticsForPartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getDeleteColumnStatisticsForPartition()
    {
        return deleteColumnStatisticsForPartition;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetUserDefinedFunction()
    {
        return getUserDefinedFunction;
    }

    @Managed
    @Nested
    public AwsApiCallStats getGetUserDefinedFunctions()
    {
        return getUserDefinedFunctions;
    }

    @Managed
    @Nested
    public AwsApiCallStats getCreateUserDefinedFunction()
    {
        return createUserDefinedFunction;
    }

    @Managed
    @Nested
    public AwsApiCallStats getUpdateUserDefinedFunction()
    {
        return updateUserDefinedFunction;
    }

    @Managed
    @Nested
    public AwsApiCallStats getDeleteUserDefinedFunction()
    {
        return deleteUserDefinedFunction;
    }

    public MetricPublisher newMetricsPublisher()
    {
        return new JmxMetricPublisher(this);
    }

    public static class JmxMetricPublisher
            implements MetricPublisher
    {
        private static final Logger log = Logger.get(JmxMetricPublisher.class);

        private static final Set<SdkMetric<?>> ALLOWED_METRICS = Set.of(API_CALL_SUCCESSFUL, RETRY_COUNT, API_CALL_DURATION, ERROR_TYPE);
        private final GlueMetastoreStats stats;

        public JmxMetricPublisher(GlueMetastoreStats stats)
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
                    return;
                }
                if (!serviceId.get().equalsIgnoreCase("glue")) {
                    return;
                }
                AwsApiCallStats apiCallStats = getApiCallStats(operationName.get());
                publishMetrics(metricCollection, apiCallStats);
            }
            catch (Exception e) {
                log.warn(e, "Publishing AWS Glue metrics failed");
            }
        }

        private void publishMetrics(MetricCollection metricCollection, AwsApiCallStats apiCallStats)
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

        private AwsApiCallStats getApiCallStats(String operationName)
        {
            return switch (operationName) {
                case "GetDatabases" -> stats.getDatabases;
                case "GetDatabase" -> stats.getDatabase;
                case "GetTables" -> stats.getTables;
                case "GetTable" -> stats.getTable;
                case "CreateDatabase" -> stats.createDatabase;
                case "DeleteDatabase" -> stats.deleteDatabase;
                case "UpdateDatabase" -> stats.updateDatabase;
                case "CreateTable" -> stats.createTable;
                case "DeleteTable" -> stats.deleteTable;
                case "UpdateTable" -> stats.updateTable;
                case "GetPartitionNames" -> stats.getPartitionNames;
                case "GetPartitions" -> stats.getPartitions;
                case "GetPartition" -> stats.getPartition;
                case "CreatePartitions" -> stats.createPartitions;
                case "DeletePartition" -> stats.deletePartition;
                case "UpdatePartition" -> stats.updatePartition;
                case "BatchUpdatePartition" -> stats.batchUpdatePartition;
                case "BatchCreatePartition" -> stats.batchCreatePartition;
                case "GetColumnStatisticsForTable" -> stats.getColumnStatisticsForTable;
                case "GetColumnStatisticsForPartition" -> stats.getColumnStatisticsForPartition;
                case "UpdateColumnStatisticsForTable" -> stats.updateColumnStatisticsForTable;
                case "DeleteColumnStatisticsForTable" -> stats.deleteColumnStatisticsForTable;
                case "UpdateColumnStatisticsForPartition" -> stats.updateColumnStatisticsForPartition;
                case "DeleteColumnStatisticsForPartition" -> stats.deleteColumnStatisticsForPartition;
                case "GetUserDefinedFunction" -> stats.getUserDefinedFunction;
                case "GetUserDefinedFunctions" -> stats.getUserDefinedFunctions;
                case "CreateUserDefinedFunction" -> stats.createUserDefinedFunction;
                case "UpdateUserDefinedFunction" -> stats.updateUserDefinedFunction;
                case "DeleteUserDefinedFunction" -> stats.deleteUserDefinedFunction;
                default -> stats.dummy;
            };
        }
    }

    private static class DummyAwsApiCallStats
            extends AwsApiCallStats
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
