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

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.ConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.s3.HiveS3Config;
import io.prestosql.plugin.hive.s3.PrestoS3ConfigurationInitializer;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeEncryptionMaterialsProvider.setQueryStageMasterKey;
import static io.prestosql.plugin.hive.DynamicConfigurationProvider.setCacheKey;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SESSION_TOKEN;
import static java.util.Objects.requireNonNull;

final class HiveUtils
{
    static List<HiveColumnHandle> getHiveColumnHandles(TypeTranslator typeTranslator, List<JdbcColumnHandle> columns)
    {
        return IntStream.range(0, columns.size())
                .mapToObj(index -> toHiveColumnHandle(typeTranslator, columns.get(index), index))
                .collect(toImmutableList());
    }

    private static HiveColumnHandle toHiveColumnHandle(TypeTranslator typeTranslator, JdbcColumnHandle jdbcColumnHandle, int columnIndex)
    {
        return new HiveColumnHandle(
                jdbcColumnHandle.getColumnName(),
                toHiveType(typeTranslator, jdbcColumnHandle.getColumnType()),
                jdbcColumnHandle.getColumnType(),
                columnIndex,
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
        final String s3AwsSessionToken;
        final Optional<String> queryStageMasterKey;

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
