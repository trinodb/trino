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
import io.trino.filesystem.Location;
import io.trino.operator.TestScanFilterAndProjectOperator;
import io.trino.plugin.hive.coercions.CoercionUtils;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HivePageSourceProvider.createBucketValidator;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHivePageSource
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorPageSource.class, HivePageSource.class);
    }

    @Test
    public void testGetNextPageSucceedsWhenHiveBucketingEnabled()
            throws IOException
    {
        testGetNextPageWhenHiveBucketingEnabled(OptionalInt.of(1));
    }

    @Test
    public void testGetNextPageThrowsExceptionWhenHiveBucketingEnabled()
    {
        assertThatThrownBy(() -> testGetNextPageWhenHiveBucketingEnabled(OptionalInt.of(-1)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Hive table is corrupt.");
    }

    private void testGetNextPageWhenHiveBucketingEnabled(OptionalInt tableBucketNumber)
            throws IOException
    {
        String partitionColumnName = "partition_col";
        String bucketColumnName = "bucket_col";

        List<HiveColumnHandle> columns = ImmutableList.of(
                new HiveColumnHandle(partitionColumnName, 0, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty()),
                new HiveColumnHandle("regular_col", 1, HIVE_STRING, VARCHAR, Optional.empty(), REGULAR, Optional.empty()),
                new HiveColumnHandle(bucketColumnName, 2, HIVE_LONG, BIGINT, Optional.empty(), REGULAR, Optional.empty()));

        String partitionColumnValue = "1";
        List<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey(partitionColumnName, partitionColumnValue));
        List<HivePageSourceProvider.ColumnMapping> columnMappings = HivePageSourceProvider.ColumnMapping.buildColumnMappings(
                partitionColumnValue,
                partitionKeys,
                columns,
                ImmutableList.of(),
                ImmutableMap.of(),
                null,
                tableBucketNumber,
                0,
                0);

        List<HivePageSourceProvider.ColumnMapping> regularAndInterimColumnMappings = HivePageSourceProvider.ColumnMapping.extractRegularAndInterimColumnMappings(columnMappings);

        List<HiveColumnHandle> bucketColumns = columns.stream().filter(c -> c.getName().equals(bucketColumnName)).toList();
        Optional<HiveSplit.BucketValidation> bucketValidation = Optional.of(new HiveSplit.BucketValidation(BUCKETING_V1, 8, bucketColumns));
        Optional<HivePageSource.BucketValidator> bucketValidator = createBucketValidator(
                Location.of("memory:///test"),
                bucketValidation,
                tableBucketNumber,
                regularAndInterimColumnMappings);

        Block[] blocks = new Block[] {
                nativeValueToBlock(VARCHAR, utf8Slice("a")),
                nativeValueToBlock(BIGINT, 1L)
        };
        Page page = new Page(1, blocks);

        try (
                ConnectorPageSource pageSource = new TestScanFilterAndProjectOperator.SinglePagePageSource(page);
                HivePageSource hivePageSource = new HivePageSource(
                        columnMappings,
                        Optional.empty(),
                        bucketValidator,
                        Optional.empty(),
                        TESTING_TYPE_MANAGER,
                        new CoercionUtils.CoercionContext(DEFAULT_PRECISION, PARQUET),
                        pageSource)) {
            hivePageSource.getNextPage();
        }
    }
}
