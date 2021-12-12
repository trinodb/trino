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
import io.airlift.units.Duration;
import io.trino.plugin.hive.aws.AbstractSdkMetricsCollector;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GlueMetastoreStats
{
    private final GlueMetastoreApiStats getAllDatabases = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getAllTables = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getAllViews = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats createDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats dropDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats renameDatabase = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats createTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats dropTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats replaceTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartitionNames = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartitions = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartition = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getPartitionByName = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats addPartitions = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats dropPartition = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats alterPartition = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getColumnStatisticsForTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats getColumnStatisticsForPartition = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats updateColumnStatisticsForTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats deleteColumnStatisticsForTable = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats updateColumnStatisticsForPartition = new GlueMetastoreApiStats();
    private final GlueMetastoreApiStats deleteColumnStatisticsForPartition = new GlueMetastoreApiStats();

    // see AWSRequestMetrics
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
    public GlueMetastoreApiStats getGetAllDatabases()
    {
        return getAllDatabases;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetAllTables()
    {
        return getAllTables;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetAllViews()
    {
        return getAllViews;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getCreateDatabase()
    {
        return createDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDropDatabase()
    {
        return dropDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getRenameDatabase()
    {
        return renameDatabase;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDropTable()
    {
        return dropTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getReplaceTable()
    {
        return replaceTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartitionNames()
    {
        return getPartitionNames;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartitions()
    {
        return getPartitions;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartition()
    {
        return getPartition;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetPartitionByName()
    {
        return getPartitionByName;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getAddPartitions()
    {
        return addPartitions;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDropPartition()
    {
        return dropPartition;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getAlterPartition()
    {
        return alterPartition;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetColumnStatisticsForTable()
    {
        return getColumnStatisticsForTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getGetColumnStatisticsForPartition()
    {
        return getColumnStatisticsForPartition;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getUpdateColumnStatisticsForTable()
    {
        return updateColumnStatisticsForTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDeleteColumnStatisticsForTable()
    {
        return deleteColumnStatisticsForTable;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getUpdateColumnStatisticsForPartition()
    {
        return updateColumnStatisticsForPartition;
    }

    @Managed
    @Nested
    public GlueMetastoreApiStats getDeleteColumnStatisticsForPartition()
    {
        return deleteColumnStatisticsForPartition;
    }

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

    public GlueSdkClientMetricsCollector newRequestMetricsCollector()
    {
        return new GlueSdkClientMetricsCollector(this);
    }

    public static class GlueSdkClientMetricsCollector
            extends AbstractSdkMetricsCollector
    {
        private final GlueMetastoreStats stats;

        public GlueSdkClientMetricsCollector(GlueMetastoreStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        protected void recordRequestCount(long count)
        {
            stats.awsRequestCount.update(count);
        }

        @Override
        protected void recordRetryCount(long count)
        {
            stats.awsRetryCount.update(count);
        }

        @Override
        protected void recordThrottleExceptionCount(long count)
        {
            stats.awsThrottleExceptions.update(count);
        }

        @Override
        protected void recordHttpRequestTime(Duration duration)
        {
            stats.awsRequestTime.add(duration);
        }

        @Override
        protected void recordClientExecutionTime(Duration duration)
        {
            stats.awsClientExecuteTime.add(duration);
        }

        @Override
        protected void recordRetryPauseTime(Duration duration)
        {
            stats.awsClientRetryPauseTime.add(duration);
        }

        @Override
        protected void recordHttpClientPoolAvailableCount(long count)
        {
            stats.awsHttpClientPoolAvailableCount.set(count);
        }

        @Override
        protected void recordHttpClientPoolLeasedCount(long count)
        {
            stats.awsHttpClientPoolLeasedCount.set(count);
        }

        @Override
        protected void recordHttpClientPoolPendingCount(long count)
        {
            stats.awsHttpClientPoolPendingCount.set(count);
        }
    }
}
