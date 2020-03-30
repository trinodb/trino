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
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.services.s3.internal.crypto.JceEncryptionConstants.SYMMETRIC_CIPHER_BLOCK_SIZE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getHdfsEnvironment;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getHiveColumnHandles;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SnowflakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TypeManager typeManager;
    private final SnowflakeHiveTypeTranslator typeTranslator = new SnowflakeHiveTypeTranslator();
    private final FileFormatDataSourceStats stats;
    private final HiveConfig hiveConfig;
    private final ParquetReaderConfig parquetReaderConfig;

    @Inject
    public SnowflakePageSourceProvider(TypeManager typeManager, FileFormatDataSourceStats stats, SnowflakeDistributedConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.hiveConfig = requireNonNull(config, "config is null").getHiveConfig();
        this.parquetReaderConfig = new ParquetReaderConfig().setMaxReadBlockSize(config.getParquetMaxReadBlockSize());
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        SnowflakeSplit snowflakeSplit = (SnowflakeSplit) split;
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(
                new HdfsConfig(),
                snowflakeSplit.getS3AwsAccessKey(),
                snowflakeSplit.getS3AwsSecretKey(),
                snowflakeSplit.getS3AwsSessionToken(),
                Optional.of(snowflakeSplit.getQueryStageMasterKey()));
        SnowflakePageSourceFactory parquetPageSourceFactory = new SnowflakePageSourceFactory(
                new ParquetPageSourceFactory(
                        typeManager,
                        hdfsEnvironment,
                        stats,
                        parquetReaderConfig));
        Path path = new Path(snowflakeSplit.getPath());
        Configuration configuration = hdfsEnvironment.getConfiguration(
                new HdfsContext(session, snowflakeSplit.getDatabase(), snowflakeSplit.getTable()),
                path);

        int paddedBytes = getPaddedBytes(session, hdfsEnvironment, configuration, snowflakeSplit, path);
        long unpaddedFileSize = snowflakeSplit.getFileSize() - paddedBytes;

        return parquetPageSourceFactory.createPageSource(
                configuration,
                session,
                path,
                snowflakeSplit.getStart(),
                snowflakeSplit.getLength(),
                unpaddedFileSize,
                snowflakeSplit.getSchema(),
                getHiveColumnHandles(
                        typeTranslator,
                        columns.stream()
                                .map(JdbcColumnHandle.class::cast)
                                .collect(toImmutableList())),
                snowflakeSplit.getEffectivePredicate(),
                hiveConfig.getDateTimeZone(),
                Optional.empty()).orElseThrow(() -> new IllegalStateException("Could not create Parquet page source"));
    }

    // for more information see https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS%235_and_PKCS%237
    private int getPaddedBytes(ConnectorSession session, HdfsEnvironment hdfsEnvironment, Configuration configuration, SnowflakeSplit split, Path path)
    {
        try (FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration).open(path)) {
            byte[] buffer = new byte[SYMMETRIC_CIPHER_BLOCK_SIZE];
            int readBytes = inputStream.read(split.getFileSize() - SYMMETRIC_CIPHER_BLOCK_SIZE, buffer, 0, SYMMETRIC_CIPHER_BLOCK_SIZE);

            if (readBytes > 0) {
                return SYMMETRIC_CIPHER_BLOCK_SIZE - readBytes;
            }
            else {
                // entire last block is padded
                return SYMMETRIC_CIPHER_BLOCK_SIZE;
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, format("Error during obtaining padding length for Hive split %s: %s", path, e.getMessage()), e);
        }
    }
}
