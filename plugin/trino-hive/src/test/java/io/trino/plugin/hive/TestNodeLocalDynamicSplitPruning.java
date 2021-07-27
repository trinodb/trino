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
import io.airlift.testing.TempFile;
import io.trino.connector.CatalogName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveRecordCursorProviders;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;

public class TestNodeLocalDynamicSplitPruning
{
    private static final String HIVE_CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";
    private static final Column BUCKET_COLUMN = new Column("l_orderkey", HIVE_INT, Optional.empty());
    private static final HiveColumnHandle BUCKET_HIVE_COLUMN_HANDLE = new HiveColumnHandle(
            BUCKET_COLUMN.getName(),
            0,
            BUCKET_COLUMN.getType(),
            BUCKET_COLUMN.getType().getType(TYPE_MANAGER),
            Optional.empty(),
            REGULAR,
            Optional.empty());

    @Test
    public void testDynamicBucketPruning()
            throws IOException
    {
        HiveConfig config = new HiveConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle();
        try (TempFile tempFile = new TempFile()) {
            ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, config, tempFile.file(), getDynamicFilter(getTupleDomainForSplitPruning()));
            assertEquals(emptyPageSource.getClass(), EmptyPageSource.class);

            ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, config, tempFile.file(), getDynamicFilter(getNonSelectiveTupleDomain()));
            assertEquals(nonEmptyPageSource.getClass(), HivePageSource.class);
        }
    }

    private static ConnectorPageSource createTestingPageSource(HiveTransactionHandle transaction, HiveConfig hiveConfig, File outputFile, DynamicFilter dynamicFilter)
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, hiveConfig.getHiveStorageFormat().getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, hiveConfig.getHiveStorageFormat().getSerDe());
        HiveSplit split = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                "",
                "file:///" + outputFile.getAbsolutePath(),
                0,
                outputFile.length(),
                outputFile.length(),
                outputFile.lastModified(),
                splitProperties,
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.of(1),
                0,
                false,
                TableToPartitionMapping.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                0);

        TableHandle tableHandle = new TableHandle(
                new CatalogName(HIVE_CATALOG_NAME),
                new HiveTableHandle(
                        SCHEMA_NAME,
                        TABLE_NAME,
                        ImmutableMap.of(),
                        ImmutableList.of(),
                        ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE),
                        Optional.of(new HiveBucketHandle(
                                ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE),
                                BUCKETING_V1,
                                20,
                                20,
                                ImmutableList.of()))),
                transaction,
                Optional.empty());

        HivePageSourceProvider provider = new HivePageSourceProvider(
                TYPE_MANAGER,
                HDFS_ENVIRONMENT,
                hiveConfig,
                getDefaultHivePageSourceFactories(HDFS_ENVIRONMENT, hiveConfig),
                getDefaultHiveRecordCursorProviders(hiveConfig, HDFS_ENVIRONMENT),
                new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT, hiveConfig),
                Optional.empty());

        return provider.createPageSource(
                transaction,
                getSession(hiveConfig),
                split,
                tableHandle.getConnectorHandle(),
                ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE),
                dynamicFilter);
    }

    private static TupleDomain<ColumnHandle> getTupleDomainForSplitPruning()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        BUCKET_HIVE_COLUMN_HANDLE,
                        Domain.singleValue(INTEGER, 10L)));
    }

    private static TupleDomain<ColumnHandle> getNonSelectiveTupleDomain()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        BUCKET_HIVE_COLUMN_HANDLE,
                        Domain.singleValue(INTEGER, 1L)));
    }

    private static TestingConnectorSession getSession(HiveConfig config)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new HiveSessionProperties(config, new OrcReaderConfig(), new OrcWriterConfig(), new ParquetReaderConfig(), new ParquetWriterConfig()).getSessionProperties())
                .build();
    }

    private static DynamicFilter getDynamicFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        return new DynamicFilter()
        {
            @Override
            public CompletableFuture<?> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return tupleDomain;
            }
        };
    }
}
