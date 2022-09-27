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

import com.amazonaws.metrics.RequestMetricCollector;
import io.trino.plugin.hive.aws.AwsApiCallStats;
import io.trino.plugin.hive.aws.AwsSdkClientCoreStats;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class GlueMetastoreStats
{
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
    private final AwsApiCallStats getPartitionByName = new AwsApiCallStats();
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

    private final AwsSdkClientCoreStats clientCoreStats = new AwsSdkClientCoreStats();

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
    public AwsApiCallStats getGetPartitionByName()
    {
        return getPartitionByName;
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
    @Flatten
    public AwsSdkClientCoreStats getClientCoreStats()
    {
        return clientCoreStats;
    }

    public RequestMetricCollector newRequestMetricsCollector()
    {
        return clientCoreStats.newRequestMetricCollector();
    }
}
