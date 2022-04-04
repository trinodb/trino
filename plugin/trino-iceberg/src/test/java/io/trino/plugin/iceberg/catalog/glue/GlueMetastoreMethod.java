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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.math.DoubleMath;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreApiStats;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;

import java.math.RoundingMode;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public enum GlueMetastoreMethod
{
    BATCH_CREATE_PARTITION(GlueMetastoreStats::getBatchCreatePartition),
    BATCH_UPDATE_PARTITION(GlueMetastoreStats::getBatchUpdatePartition),
    CREATE_DATABASE(GlueMetastoreStats::getCreateDatabase),
    CREATE_PARTITIONS(GlueMetastoreStats::getCreatePartitions),
    CREATE_TABLE(GlueMetastoreStats::getCreateTable),
    DELETE_COLUMN_STATISTICS_FOR_PARTITION(GlueMetastoreStats::getDeleteColumnStatisticsForPartition),
    DELETE_COLUMN_STATISTICS_FOR_TABLE(GlueMetastoreStats::getDeleteColumnStatisticsForTable),
    DELETE_DATA_BASE(GlueMetastoreStats::getDeleteDatabase),
    DELETE_PARTITION(GlueMetastoreStats::getDeletePartition),
    DELETE_TABLE(GlueMetastoreStats::getDeleteTable),
    GET_COLUMN_STATISTICS_FOR_PARTITION(GlueMetastoreStats::getGetColumnStatisticsForPartition),
    GET_COLUMN_STATISTICS_FOR_TABLE(GlueMetastoreStats::getDeleteColumnStatisticsForTable),
    GET_DATABASE(GlueMetastoreStats::getGetDatabase),
    GET_DATABASES(GlueMetastoreStats::getGetDatabases),
    GET_PARTITION(GlueMetastoreStats::getGetPartition),
    GET_PARTITION_BY_NAME(GlueMetastoreStats::getGetPartitionByName),
    GET_PARTITION_NAMES(GlueMetastoreStats::getGetPartitionNames),
    GET_PARTITIONS(GlueMetastoreStats::getGetPartitions),
    GET_TABLE(GlueMetastoreStats::getGetTable),
    GET_TABLES(GlueMetastoreStats::getGetTables),
    UPDATE_COLUMN_STATISTICS_FOR_PARTITION(GlueMetastoreStats::getUpdateColumnStatisticsForPartition),
    UPDATE_COLUMN_STATISTICS_FOR_TABLE(GlueMetastoreStats::getUpdateColumnStatisticsForTable),
    UPDATE_DATABASE(GlueMetastoreStats::getUpdateDatabase),
    UPDATE_PARTITION(GlueMetastoreStats::getUpdatePartition),
    UPDATE_TABLE(GlueMetastoreStats::getUpdateTable),
    /**/;

    private final Function<GlueMetastoreStats, GlueMetastoreApiStats> extractor;

    GlueMetastoreMethod(Function<GlueMetastoreStats, GlueMetastoreApiStats> extractor)
    {
        this.extractor = requireNonNull(extractor, "extractor is null");
    }

    public GlueMetastoreApiStats getStatFrom(GlueMetastoreStats stats)
    {
        return this.extractor.apply(stats);
    }

    public int getInvocationCount(GlueMetastoreStats stats)
    {
        double count = this.getStatFrom(stats).getTime().getAllTime().getCount();
        return DoubleMath.roundToInt(count, RoundingMode.UNNECESSARY);
    }
}
