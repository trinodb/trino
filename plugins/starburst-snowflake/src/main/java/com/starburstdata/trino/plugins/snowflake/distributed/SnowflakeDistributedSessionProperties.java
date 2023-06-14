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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Objects.requireNonNull;

public class SnowflakeDistributedSessionProperties
        implements SessionPropertiesProvider
{
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";
    private static final String FORCE_LOCAL_SCHEDULING = "force_local_scheduling";
    private static final String IGNORE_ABSENT_PARTITIONS = "ignore_absent_partitions";
    private static final String PARTITION_USE_COLUMN_NAMES = "partition_use_column_names";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_USE_COLUMN_INDEX = "parquet_use_column_index";
    private static final String RETRY_CANCELED_QUERIES = "retry_canceled_queries";
    private static final String VALIDATE_BUCKETING = "validate_bucketing";
    private static final String OPTIMIZE_SYMLINK_LISTING = "optimize_symlink_listing";
    private static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";

    private final SnowflakeDistributedConfig snowflakeConfig;

    @Inject
    public SnowflakeDistributedSessionProperties(SnowflakeDistributedConfig snowflakeConfig)
    {
        this.snowflakeConfig = requireNonNull(snowflakeConfig, "snowflakeConfig is null");
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.of(
                // Hive session properties copied from io.trino.plugin.hive.HiveSessionProperties
                dataSizeProperty(
                        MAX_INITIAL_SPLIT_SIZE,
                        "Max initial split size",
                        snowflakeConfig.getMaxInitialSplitSize(),
                        true),
                dataSizeProperty(
                        MAX_SPLIT_SIZE,
                        "Max split size",
                        snowflakeConfig.getMaxSplitSize(),
                        true),
                dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        snowflakeConfig.getParquetMaxReadBlockSize(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_INDEX,
                        "Parquet: Use Parquet column index",
                        snowflakeConfig.isUseColumnIndex(),
                        false),
                booleanProperty(
                        RETRY_CANCELED_QUERIES,
                        "Retry queries that failed due to being canceled",
                        snowflakeConfig.isRetryCanceledQueries(),
                        false),
                // these properties are irrelevant for Snowflake connector, but are required by Hive connector code
                booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "Internal Snowflake connector property",
                        false,
                        value -> checkArgument(!value, "Enabling s3_select_pushdown_enabled not supported for Snowflake"),
                        true),
                booleanProperty(
                        FORCE_LOCAL_SCHEDULING,
                        "Internal Snowflake connector property",
                        false,
                        value -> checkArgument(!value, "Enabling force_local_scheduling not supported for Snowflake"),
                        true),
                booleanProperty(
                        IGNORE_ABSENT_PARTITIONS,
                        "Internal Snowflake connector property",
                        false,
                        value -> checkArgument(!value, "Enabling ignore_absent_partitions not supported for Snowflake"),
                        true),
                booleanProperty(
                        PARTITION_USE_COLUMN_NAMES,
                        "Internal Snowflake connector property",
                        false,
                        value -> checkArgument(!value, "Enabling partition_use_column_names not supported for Snowflake"),
                        true),
                booleanProperty(
                        VALIDATE_BUCKETING,
                        "Internal Snowflake connector property",
                        false,
                        value -> checkArgument(!value, "Enabling validate_bucketing not supported for Snowflake"),
                        true),
                booleanProperty(OPTIMIZE_SYMLINK_LISTING,
                        "Internal Snowflake connector property",
                        false,
                        value -> checkArgument(!value, "Enabling optimize_symlink_listing not supported for Snowflake"),
                        true),
                booleanProperty(
                        SIZE_BASED_SPLIT_WEIGHTS_ENABLED,
                        "Internal Snowflake connector property",
                        false,
                        value -> checkArgument(!value, "Enabling size_based_split_weights_enabled not supported for Snowflake"),
                        true));
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static boolean isParquetUseColumnIndex(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_INDEX, Boolean.class);
    }

    public static boolean retryCanceledQueries(ConnectorSession session)
    {
        return session.getProperty(RETRY_CANCELED_QUERIES, Boolean.class);
    }
}
