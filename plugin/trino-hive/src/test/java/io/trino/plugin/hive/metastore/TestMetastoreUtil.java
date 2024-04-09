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
package io.trino.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.bucketColumnHandle;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getBasicStatisticsWithSparkFallback;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetastoreUtil
{
    @Test
    public void testComputePartitionKeyFilter()
    {
        HiveColumnHandle dsColumn = partitionColumn("ds");
        HiveColumnHandle typeColumn = partitionColumn("type");
        List<HiveColumnHandle> partitionKeys = ImmutableList.of(dsColumn, typeColumn);

        Domain dsDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("2018-05-06"))), false);
        Domain typeDomain = Domain.create(ValueSet.of(VARCHAR, utf8Slice("fruit")), false);

        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<HiveColumnHandle, Domain>builder()
                .put(bucketColumnHandle(), Domain.create(ValueSet.of(INTEGER, 123L), false))
                .put(dsColumn, dsDomain)
                .put(typeColumn, typeDomain)
                .buildOrThrow());

        TupleDomain<String> filter = computePartitionKeyFilter(partitionKeys, tupleDomain);
        assertThat(filter.getDomains())
                .as("output contains only the partition keys")
                .contains(ImmutableMap.<String, Domain>builder()
                        .put("ds", dsDomain)
                        .put("type", typeDomain)
                        .buildOrThrow());
    }

    @Test
    public void testBasicStatisticsRoundTrip()
    {
        testBasicStatisticsRoundTrip(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()));
        testBasicStatisticsRoundTrip(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.empty(), OptionalLong.of(2), OptionalLong.empty()));
        testBasicStatisticsRoundTrip(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)));
    }

    private static void testBasicStatisticsRoundTrip(HiveBasicStatistics expected)
    {
        assertThat(getHiveBasicStatistics(updateStatisticsParameters(ImmutableMap.of(), expected))).isEqualTo(expected);
    }

    @Test
    public void testSparkFallbackGetBasicStatistics()
    {
        // only spark stats
        Map<String, String> tableParameters = Map.of(
                "spark.sql.statistics.numFiles", "1",
                "spark.sql.statistics.numRows", "2",
                "spark.sql.statistics.rawDataSize", "3",
                "spark.sql.statistics.totalSize", "4");
        HiveBasicStatistics actual = getBasicStatisticsWithSparkFallback(tableParameters);
        assertThat(actual).isEqualTo(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)));
        actual = getHiveBasicStatistics(tableParameters);
        assertThat(actual).isEqualTo(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()));
        // empty hive and not empty spark stats
        tableParameters = Map.of(
                "numFiles", "0",
                "numRows", "0",
                "rawDataSize", "0",
                "totalSize", "0",
                "spark.sql.statistics.numFiles", "1",
                "spark.sql.statistics.numRows", "2",
                "spark.sql.statistics.rawDataSize", "3",
                "spark.sql.statistics.totalSize", "4");
        actual = getBasicStatisticsWithSparkFallback(tableParameters);
        assertThat(actual).isEqualTo(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)));
        actual = getHiveBasicStatistics(tableParameters);
        assertThat(actual).isEqualTo(new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0)));
        //  not empty hive and not empty spark stats
        tableParameters = Map.of(
                "numFiles", "10",
                "numRows", "20",
                "rawDataSize", "30",
                "totalSize", "40",
                "spark.sql.statistics.numFiles", "1",
                "spark.sql.statistics.numRows", "2",
                "spark.sql.statistics.rawDataSize", "3",
                "spark.sql.statistics.totalSize", "4");
        actual = getBasicStatisticsWithSparkFallback(tableParameters);
        assertThat(actual).isEqualTo(new HiveBasicStatistics(OptionalLong.of(10), OptionalLong.of(20), OptionalLong.of(30), OptionalLong.of(40)));
    }

    private static HiveColumnHandle partitionColumn(String name)
    {
        return new HiveColumnHandle(name, 0, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty());
    }
}
