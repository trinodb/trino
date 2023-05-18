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

import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.starburstdata.trino.plugins.snowflake.distributed.S3EncryptionMaterialsProvider.configureClientSideEncryption;
import static com.starburstdata.trino.plugins.snowflake.distributed.SnowflakeHiveTypeTranslator.toHiveType;
import static io.trino.hdfs.DynamicConfigurationProvider.setCacheKey;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SESSION_TOKEN;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType.AZURE;
import static net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType.S3;

public final class HiveUtils
{
    private static final String QUERY_STAGE_MASTER_KEY = "snowflake.encryption-material";
    private static final String AWS_KEY_ID = "AWS_KEY_ID";
    private static final String AWS_SECRET_KEY = "AWS_SECRET_KEY";
    private static final String AWS_TOKEN = "AWS_TOKEN";
    private static final String AZURE_SAS_TOKEN = "AZURE_SAS_TOKEN";
    private static final Set<StageInfo.StageType> SUPPORTED_STAGE_TYPES = ImmutableSet.of(S3, AZURE);

    public static void validateStageType(StageInfo.StageType stageType)
    {
        checkArgument(SUPPORTED_STAGE_TYPES.contains(stageType), "Unsupported Snowflake Stage type: " + stageType);
    }

    public static List<HiveColumnHandle> getHiveColumnHandles(List<JdbcColumnHandle> columns)
    {
        return IntStream.range(0, columns.size())
                .mapToObj(index -> toHiveColumnHandle(columns.get(index), index))
                .collect(toImmutableList());
    }

    public static String getStageLocation(SnowflakeFileTransferAgent transferAgent)
    {
        switch (transferAgent.getStageInfo().getStageType()) {
            case S3:
                return "s3://" + transferAgent.getStageLocation();
            case AZURE:
                // TransferAgent's stageLocation is in <container>/<path> format
                String container = extractWasbContainer(transferAgent.getStageLocation());
                String formattedLocation = format("wasbs://%s@%s.%s", container, transferAgent.getStageInfo().getStorageAccount(), transferAgent.getStageInfo().getEndPoint());
                String path = extractWasbPath(transferAgent.getStageLocation());
                if (!path.isEmpty()) {
                    // Ensure we do not have trailing `/` present
                    // Without this code computation if file is a direct child of table location in io.trino.plugin.hive.fs.DirectoryListingFilter.findNextElement is wrong.
                    // The table location has trailing slash and location computed with fileLocation.getParent() does not.
                    formattedLocation = formattedLocation + "/" + path;
                }
                return formattedLocation;
            default:
                throw new IllegalStateException("Unsupported stage type " + transferAgent.getStageInfo().getStageType());
        }
    }

    public static HdfsEnvironment getHdfsEnvironment(SnowflakeSplit snowflakeSplit)
    {
        switch (snowflakeSplit.getStageAccessInfo().getStageType()) {
            case S3:
                return getHdfsS3Environment(
                        new HdfsConfig(),
                        snowflakeSplit.getStageAccessInfo().getS3AwsAccessKey(),
                        snowflakeSplit.getStageAccessInfo().getS3AwsSecretKey(),
                        snowflakeSplit.getStageAccessInfo().getS3AwsSessionToken(),
                        Optional.of(snowflakeSplit.getStageAccessInfo().getQueryStageMasterKey()));
            case AZURE:
                return getHdfsAzureEnvironment(
                        new HdfsConfig(),
                        snowflakeSplit.getStageAccessInfo().getWasbContainer(),
                        snowflakeSplit.getStageAccessInfo().getWasbSasKey(),
                        snowflakeSplit.getStageAccessInfo().getWasbStorageAccount(),
                        snowflakeSplit.getStageAccessInfo().getWasbEndPoint(),
                        Optional.of(snowflakeSplit.getStageAccessInfo().getQueryStageMasterKey()));
            default:
                throw new IllegalStateException("Unsupported stage type: " + snowflakeSplit.getStageAccessInfo().getStageType());
        }
    }

    public static SnowflakeStageAccessInfo getSnowflakeStageAccessInfo(SnowflakeFileTransferAgent transferAgent)
    {
        switch (transferAgent.getStageInfo().getStageType()) {
            case S3:
                return SnowflakeStageAccessInfo.forS3(
                        getCredential(transferAgent.getStageInfo(), AWS_KEY_ID),
                        getCredential(transferAgent.getStageInfo(), AWS_SECRET_KEY),
                        getCredential(transferAgent.getStageInfo(), AWS_TOKEN),
                        getQueryStageMasterKey(transferAgent));
            case AZURE:
                return SnowflakeStageAccessInfo.forAzure(
                        extractWasbContainer(transferAgent.getStageLocation()),
                        getCredential(transferAgent.getStageInfo(), AZURE_SAS_TOKEN),
                        transferAgent.getStageInfo().getStorageAccount(),
                        transferAgent.getStageInfo().getEndPoint(),
                        getQueryStageMasterKey(transferAgent));
            default:
                throw new IllegalStateException("Unsupported stage type: " + transferAgent.getStageInfo().getStageType());
        }
    }

    public static HdfsEnvironment getHdfsEnvironment(HdfsConfig hdfsConfig, SnowflakeFileTransferAgent transferAgent)
    {
        switch (transferAgent.getStageInfo().getStageType()) {
            case S3:
                return getHdfsS3Environment(
                        hdfsConfig,
                        getCredential(transferAgent.getStageInfo(), AWS_KEY_ID),
                        getCredential(transferAgent.getStageInfo(), AWS_SECRET_KEY),
                        getCredential(transferAgent.getStageInfo(), AWS_TOKEN),
                        Optional.of(getQueryStageMasterKey(transferAgent)));
            case AZURE:
                String stageLocation = transferAgent.getStageLocation();
                return getHdfsAzureEnvironment(
                        hdfsConfig,
                        extractWasbContainer(stageLocation),
                        getCredential(transferAgent.getStageInfo(), AZURE_SAS_TOKEN),
                        transferAgent.getStageInfo().getStorageAccount(),
                        transferAgent.getStageInfo().getEndPoint(),
                        Optional.of(getQueryStageMasterKey(transferAgent)));
            default:
                throw new IllegalStateException("Unsupported stage type: " + transferAgent.getStageInfo().getStageType());
        }
    }

    public static String getCredential(StageInfo stageInfo, String credentialName)
    {
        return requireNonNull(stageInfo.getCredentials().get(credentialName), format("%s is null", credentialName)).toString();
    }

    public static String getQueryStageMasterKey(SnowflakeFileTransferAgent transferAgent)
    {
        return getLast(transferAgent.getEncryptionMaterial()).getQueryStageMasterKey();
    }

    public static String getQueryStageMasterKey(Configuration configuration)
    {
        return requireNonNull(configuration.get(QUERY_STAGE_MASTER_KEY), "queryStageMasterKey is null");
    }

    public static void setQueryStageMasterKey(Configuration configuration, String queryStageMasterKey)
    {
        configuration.set(QUERY_STAGE_MASTER_KEY, requireNonNull(queryStageMasterKey, "queryStageMasterKey is null"));
    }

    public static HdfsEnvironment getHdfsAzureEnvironment(
            HdfsConfig hdfsConfig,
            String wasbContainer,
            String wasbSasKey,
            String wasbStorageAccount,
            String wasbEndpoint,
            Optional<String> queryStageMasterKey)
    {
        String wasbHostname = wasbStorageAccount + "." + wasbEndpoint;
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(
                new HdfsConfigurationInitializer(hdfsConfig, ImmutableSet.of(
                        getTrinoAzureConfigurationInitializer(wasbSasKey, wasbHostname),
                        new SnowflakeAzureConfigurationInitializer(wasbContainer, wasbHostname, wasbSasKey, queryStageMasterKey))),
                ImmutableSet.of());
        return new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
    }

    public static HdfsEnvironment getHdfsS3Environment(
            HdfsConfig hdfsConfig,
            String s3AwsAccessKey,
            String s3AwsSecretKey,
            String s3AwsSessionToken,
            Optional<String> queryStageMasterKey)
    {
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(
                new HdfsConfigurationInitializer(hdfsConfig, ImmutableSet.of(
                        getTrinoS3ConfigurationInitializer(s3AwsAccessKey, s3AwsSecretKey),
                        new SetS3SessionTokenAndEncryptionMaterials(s3AwsSessionToken, queryStageMasterKey))),
                ImmutableSet.of());
        return new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
    }

    private static HiveColumnHandle toHiveColumnHandle(JdbcColumnHandle jdbcColumnHandle, int columnIndex)
    {
        return new HiveColumnHandle(
                // Snowflake supports case-sensitive column names, but does not allow collisions when compared case-insensitively.
                // The export creates Parquet files with lower-case column names.
                // TODO lowercasing is a workaround for https://github.com/trinodb/trino/issues/3574. Remove
                jdbcColumnHandle.getColumnName().toLowerCase(ENGLISH),
                columnIndex,
                toHiveType(jdbcColumnHandle.getColumnType()),
                jdbcColumnHandle.getColumnType(),
                Optional.empty(),
                REGULAR,
                Optional.empty());
    }

    private static String extractWasbContainer(String stageLocation)
    {
        return stageLocation.contains("/") ? stageLocation.substring(0, stageLocation.indexOf("/")) : stageLocation;
    }

    private static String extractWasbPath(String stageLocation)
    {
        return stageLocation.contains("/") ? stageLocation.substring(stageLocation.indexOf("/") + 1) : "";
    }

    private static TrinoAzureConfigurationInitializer getTrinoAzureConfigurationInitializer(String wasbAccessKey, String wasbHostname)
    {
        HiveAzureConfig azureConfig = new HiveAzureConfig()
                .setWasbAccessKey(wasbAccessKey)
                .setWasbStorageAccount(wasbHostname);
        return new TrinoAzureConfigurationInitializer(azureConfig);
    }

    private static class SnowflakeAzureConfigurationInitializer
            implements ConfigurationInitializer
    {
        private final String containerName;
        private final String wasbHostname;
        private final String sasKey;
        private final Optional<String> queryStageMasterKey;

        SnowflakeAzureConfigurationInitializer(String containerName, String wasbHostname, String sasKey, Optional<String> queryStageMasterKey)
        {
            this.containerName = requireNonNull(containerName, "containerName is null");
            this.wasbHostname = requireNonNull(wasbHostname, "wasbHostname is null");
            this.sasKey = requireNonNull(sasKey, "sasKey is null");
            this.queryStageMasterKey = requireNonNull(queryStageMasterKey, "queryStageMasterKey is null");
        }

        @Override
        public void initializeConfiguration(Configuration config)
        {
            config.set("fs.azure.sas." + containerName + "." + wasbHostname, sasKey);
            config.set("fs.wasbs.impl", DecryptingAzureSecureNativeFileSystem.class.getName());
            // Disable FS caching for WASBS because the cache key uses the authority part of the URI.
            // On Azure the authority part is different for each query.
            // On S3 the authority part is constant between queries.
            config.setBoolean("fs.wasbs.impl.disable.cache", true);
            checkState(queryStageMasterKey.isPresent(), "queryStageMasterKey is empty");
            queryStageMasterKey.ifPresent(key -> config.set(QUERY_STAGE_MASTER_KEY, key));
            setCacheKey(config, queryStageMasterKey.orElse(""));
        }
    }

    private static TrinoS3ConfigurationInitializer getTrinoS3ConfigurationInitializer(String s3AwsAccessKey, String s3AwsSecretKey)
    {
        HiveS3Config s3Config = new HiveS3Config()
                .setS3AwsAccessKey(s3AwsAccessKey)
                .setS3AwsSecretKey(s3AwsSecretKey);
        return new TrinoS3ConfigurationInitializer(s3Config);
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
            queryStageMasterKey.ifPresent(key -> configureClientSideEncryption(config, key));
            setCacheKey(config, sessionToken + "|" + queryStageMasterKey.orElse(""));
        }
    }

    private HiveUtils() {}
}
