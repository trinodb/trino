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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHivePageSourceProvider
{
    private static final HiveColumnHandle PARTITION_COLUMN = createBaseColumn("partition_col", 0, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
    private static final HiveColumnHandle DATA_COLUMN = createBaseColumn("data_col", 0, HIVE_INT, INTEGER, REGULAR, Optional.empty());
    private static final HiveColumnHandle BUCKET_COLUMN = createBaseColumn("bucket_col", 1, HIVE_INT, INTEGER, REGULAR, Optional.empty());
    private static final String PARTITION_NAME = "part1";
    private static final HiveBucketHandle HIVE_BUCKET_HANDLE = new HiveBucketHandle(
            ImmutableList.of(BUCKET_COLUMN),
            BUCKETING_V1,
            10,
            10,
            ImmutableList.of());
    private static final Domain DATA_DOMAIN = Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 100L, true)), false);
    private static final Domain PARTITION_DOMAIN = Domain.create(ValueSet.of(VARCHAR, utf8Slice("part1")), false);
    private static final HiveTableHandle HIVE_TABLE_HANDLE = new HiveTableHandle(
            "schema",
            "table",
            ImmutableList.of(PARTITION_COLUMN),
            ImmutableList.of(BUCKET_COLUMN, DATA_COLUMN),
            TupleDomain.withColumnDomains(ImmutableMap.of(
                    DATA_COLUMN, DATA_DOMAIN,
                    PARTITION_COLUMN, PARTITION_DOMAIN)),
            TupleDomain.all(),
            Optional.of(HIVE_BUCKET_HANDLE),
            Optional.empty(),
            Optional.empty(),
            NO_ACID_TRANSACTION);
    private static final HiveSplit HIVE_SPLIT = new HiveSplit(
            PARTITION_NAME,
            "path",
            0,
            100,
            10,
            12,
            ImmutableMap.of(),
            ImmutableList.of(new HivePartitionKey(PARTITION_COLUMN.getName(), PARTITION_NAME)),
            ImmutableList.of(),
            OptionalInt.empty(),
            OptionalInt.of(1),
            false,
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            SplitWeight.standard());
    private HivePageSourceProvider pageSourceProvider;

    @BeforeAll
    public void setup()
    {
        HiveConfig config = new HiveConfig()
                .setDomainCompactionThreshold(2);
        pageSourceProvider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                config,
                ImmutableSet.of());
    }

    @Test
    public void testSimplifyCompactsData()
    {
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L, 20L), false)))))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 20L, true)), false))));
    }

    @Test
    public void testSimplifyConsidersEffectivePredicate()
    {
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L, 110L), false)))))
                // data column domain should not be simplified because it contains only 2 values after intersection with effective predicate
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L), false))));

        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L, 12L), false)))))
                // data column domain should be simplified because it contains 3 values after intersection with effective predicate
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 12L, true)), false))));
    }

    @Test
    public void testSimplifyPrunesPartitionColumn()
    {
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        PARTITION_COLUMN, Domain.create(ValueSet.of(VARCHAR, utf8Slice("part1")), false)))))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(DATA_COLUMN, DATA_DOMAIN)));

        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        PARTITION_COLUMN, Domain.create(ValueSet.of(VARCHAR, utf8Slice("part2")), false)))))
                .isEqualTo(TupleDomain.none());
    }

    @Test
    public void testSimplifySkipsBucket()
    {
        Domain bucketDomain = Domain.create(ValueSet.of(INTEGER, 1L), false);
        TupleDomain<ColumnHandle> bucketTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                BUCKET_COLUMN, bucketDomain));
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                bucketTupleDomain))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        BUCKET_COLUMN, bucketDomain,
                        DATA_COLUMN, DATA_DOMAIN)));

        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        BUCKET_COLUMN, Domain.create(ValueSet.of(INTEGER, 2L), false)))))
                .isEqualTo(TupleDomain.none());
    }
}
