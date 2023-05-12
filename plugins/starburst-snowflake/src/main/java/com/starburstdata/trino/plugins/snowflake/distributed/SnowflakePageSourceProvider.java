/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.distributed;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.amazonaws.services.s3.internal.crypto.JceEncryptionConstants.SYMMETRIC_CIPHER_BLOCK_SIZE;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.trino.plugins.snowflake.distributed.HiveUtils.getHdfsEnvironment;
import static com.starburstdata.trino.plugins.snowflake.distributed.HiveUtils.getHiveColumnHandles;
import static com.starburstdata.trino.plugins.snowflake.distributed.HiveUtils.validateStageType;
import static com.starburstdata.trino.plugins.snowflake.distributed.SnowflakeDistributedSessionProperties.getParquetMaxReadBlockSize;
import static com.starburstdata.trino.plugins.snowflake.distributed.SnowflakeDistributedSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class SnowflakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final FileFormatDataSourceStats stats;
    private final ParquetReaderConfig parquetReaderConfig;
    // TODO should there be a config for this
    private final DateTimeZone parquetTimeZone = DateTimeZone.forID("UTC");

    @Inject
    public SnowflakePageSourceProvider(FileFormatDataSourceStats stats, SnowflakeDistributedConfig config)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.parquetReaderConfig = new ParquetReaderConfig().setMaxReadBlockSize(config.getParquetMaxReadBlockSize())
                .setUseColumnIndex(config.isUseColumnIndex());
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        SnowflakeSplit snowflakeSplit = (SnowflakeSplit) split;
        validateStageType(snowflakeSplit.getStageAccessInfo().getStageType());
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(snowflakeSplit);
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(hdfsEnvironment);

        Path path = new Path(snowflakeSplit.getPath());
        Configuration configuration = hdfsEnvironment.getConfiguration(
                new HdfsContext(session),
                path);

        int paddedBytes = getPaddedBytes(session, hdfsEnvironment, configuration, snowflakeSplit, path);
        long unpaddedFileSize = snowflakeSplit.getFileSize() - paddedBytes;

        List<HiveColumnHandle> hiveColumns = getHiveColumnHandles(
                columns.stream()
                        .map(JdbcColumnHandle.class::cast)
                        .collect(toImmutableList()));

        List<HiveColumnHandle> transformedColumns = hiveColumns.stream()
                .map(column -> {
                    if (column.getType() instanceof TimestampWithTimeZoneType) {
                        return new HiveColumnHandle(
                                column.getName(),
                                column.getBaseHiveColumnIndex(),
                                column.getHiveType(),
                                createDecimalType(19),
                                column.getHiveColumnProjectionInfo(),
                                column.getColumnType(),
                                column.getComment());
                    }
                    return column;
                })
                .collect(toImmutableList());

        Map<ColumnHandle, Integer> columnIndex = IntStream.range(0, columns.size()).boxed()
                .collect(toImmutableMap(columns::get, identity()));
        TupleDomain<HiveColumnHandle> filePredicate = TupleDomain.all();
        if (dynamicFilteringEnabled(session)) {
            filePredicate = dynamicFilter.getCurrentPredicate()
                    .filter((column, domain) -> {
                        // We transform the values, so the domain would need to be translated.
                        return !(domain.getType() instanceof TimestampWithTimeZoneType);
                    })
                    .transformKeys(handle -> {
                        JdbcColumnHandle columnHandle = (JdbcColumnHandle) handle;
                        int index = requireNonNull(columnIndex.get(columnHandle), () -> "Unexpected column: " + columnHandle);
                        return transformedColumns.get(index);
                    });
            if (filePredicate.isNone()) {
                return new EmptyPageSource();
            }
        }

        ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                fileSystemFactory.create(session).newInputFile(Location.of(snowflakeSplit.getPath()), unpaddedFileSize),
                snowflakeSplit.getStart(),
                snowflakeSplit.getLength(),
                transformedColumns,
                filePredicate,
                true,
                parquetTimeZone,
                stats,
                parquetReaderConfig.toParquetReaderOptions()
                        .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                        .withUseColumnIndex(isParquetUseColumnIndex(session))
                        .withBatchColumnReaders(false),
                Optional.empty(),
                100);

        verify(pageSource.getReaderColumns().isEmpty(), "All columns expected to be base columns");

        return new TranslatingPageSource(pageSource.get(), hiveColumns);
    }

    // for more information see https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS%235_and_PKCS%237
    private static int getPaddedBytes(ConnectorSession session, HdfsEnvironment hdfsEnvironment, Configuration configuration, SnowflakeSplit split, Path path)
    {
        try (FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(session.getIdentity(), path, configuration).open(path)) {
            byte[] buffer = new byte[SYMMETRIC_CIPHER_BLOCK_SIZE];
            int readBytes = inputStream.read(split.getFileSize() - SYMMETRIC_CIPHER_BLOCK_SIZE, buffer, 0, SYMMETRIC_CIPHER_BLOCK_SIZE);

            if (readBytes > 0) {
                return SYMMETRIC_CIPHER_BLOCK_SIZE - readBytes;
            }
            // entire last block is padded
            return SYMMETRIC_CIPHER_BLOCK_SIZE;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, format("Error during obtaining padding length for Hive split %s: %s", path, e.getMessage()), e);
        }
    }
}
