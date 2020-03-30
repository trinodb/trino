/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.PartitionSchemaEvolution;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SnowflakeDistributedSessionProperties
        implements SessionPropertiesProvider
{
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";
    private static final String FORCE_LOCAL_SCHEDULING = "force_local_scheduling";
    private static final String PARQUET_USE_COLUMN_NAME = "parquet_use_column_names";
    private static final String PARQUET_FAIL_WITH_CORRUPTED_STATISTICS = "parquet_fail_with_corrupted_statistics";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARTITION_SCHEMA_EVOLUTION = "partition_schema_evolution";

    private SnowflakeDistributedConfig snowflakeConfig;

    @Inject
    public SnowflakeDistributedSessionProperties(SnowflakeDistributedConfig snowflakeConfig)
    {
        this.snowflakeConfig = requireNonNull(snowflakeConfig, "snowflakeConfig is null");
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.of(
                // Hive session properties copied from io.prestosql.plugin.hive.HiveSessionProperties
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
                // these properties are irrelevant for Snowflake connector, but are required by Hive connector code
                booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "S3 Select pushdown enabled",
                        false,
                        true),
                booleanProperty(
                        FORCE_LOCAL_SCHEDULING,
                        "Only schedule splits on workers colocated with data node",
                        false,
                        true),
                booleanProperty(
                        PARQUET_USE_COLUMN_NAME,
                        "Experimental: Parquet: Access Parquet columns using names from the file",
                        false,
                        true),
                booleanProperty(
                        PARQUET_FAIL_WITH_CORRUPTED_STATISTICS,
                        "Parquet: Fail when scanning Parquet files with corrupted statistics",
                        true,
                        true),
                enumProperty(
                        PARTITION_SCHEMA_EVOLUTION,
                        "Determines how table and partition columns are mapped",
                        PartitionSchemaEvolution.class,
                        PartitionSchemaEvolution.BY_INDEX,
                        true));
    }

    private static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                DataSize.class,
                defaultValue,
                hidden,
                value -> DataSize.valueOf((String) value),
                DataSize::toString);
    }
}
