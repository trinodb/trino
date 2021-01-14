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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.ConfigurationInitializer;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.PrestoS3ConfigurationInitializer;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeEncryptionMaterialsProvider.setQueryStageMasterKey;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeHiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.DynamicConfigurationProvider.setCacheKey;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.s3.PrestoS3FileSystem.S3_SESSION_TOKEN;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

final class HiveUtils
{
    static List<HiveColumnHandle> getHiveColumnHandles(List<JdbcColumnHandle> columns)
    {
        return IntStream.range(0, columns.size())
                .mapToObj(index -> toHiveColumnHandle(columns.get(index), index))
                .collect(toImmutableList());
    }

    private static HiveColumnHandle toHiveColumnHandle(JdbcColumnHandle jdbcColumnHandle, int columnIndex)
    {
        return new HiveColumnHandle(
                // Snowflake supports case-sensitive column names, but does not allow collisions when compared case-insensitively.
                // The export creates Parquet files with lower-case column names.
                // TODO lowercasing is a workaround for https://github.com/prestosql/presto/issues/3574. Remove
                jdbcColumnHandle.getColumnName().toLowerCase(ENGLISH),
                columnIndex,
                toHiveType(jdbcColumnHandle.getColumnType()),
                jdbcColumnHandle.getColumnType(),
                Optional.empty(),
                REGULAR,
                Optional.empty());
    }

    static HdfsEnvironment getHdfsEnvironment(
            HdfsConfig hdfsConfig,
            String s3AwsAccessKey,
            String s3AwsSecretKey,
            String s3AwsSessionToken,
            Optional<String> queryStageMasterKey)
    {
        HiveHdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hdfsConfig, ImmutableSet.of(
                        getPrestoS3ConfigurationInitializer(s3AwsAccessKey, s3AwsSecretKey),
                        new SetS3SessionTokenAndEncryptionMaterials(s3AwsSessionToken, queryStageMasterKey))),
                ImmutableSet.of());
        return new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
    }

    private static PrestoS3ConfigurationInitializer getPrestoS3ConfigurationInitializer(String s3AwsAccessKey, String s3AwsSecretKey)
    {
        HiveS3Config s3Config = new HiveS3Config()
                .setS3AwsAccessKey(s3AwsAccessKey)
                .setS3AwsSecretKey(s3AwsSecretKey);
        return new PrestoS3ConfigurationInitializer(s3Config);
    }

    private static class SetS3SessionTokenAndEncryptionMaterials
            implements ConfigurationInitializer
    {
        private final String s3AwsSessionToken;
        private final Optional<String> queryStageMasterKey;

        SetS3SessionTokenAndEncryptionMaterials(String s3AwsSessionToken, Optional<String> queryStageMasterKey)
        {
            this.s3AwsSessionToken = requireNonNull(s3AwsSessionToken, "s3AwsSessionToken is null");
            this.queryStageMasterKey = requireNonNull(queryStageMasterKey, "queryStageMasterKey is null");
        }

        @Override
        public void initializeConfiguration(Configuration config)
        {
            String sessionToken = s3AwsSessionToken;
            config.set(S3_SESSION_TOKEN, sessionToken);
            queryStageMasterKey.ifPresent(key -> setQueryStageMasterKey(config, key));
            setCacheKey(config, sessionToken + "|" + queryStageMasterKey.orElse(""));
        }
    }

    private HiveUtils() {}
}
