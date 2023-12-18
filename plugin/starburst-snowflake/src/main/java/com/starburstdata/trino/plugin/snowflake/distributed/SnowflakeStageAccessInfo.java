/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.distributed;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;
import net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

// Snowflake stage credentials contains data necessary to access stage storage
public class SnowflakeStageAccessInfo
{
    private final StageType stageType;

    private final String s3AwsAccessKey;
    private final String s3AwsSecretKey;
    private final String s3AwsSessionToken;
    private final String queryStageMasterKey;
    private final String wasbContainer;
    private final String wasbSasKey;
    private final String wasbStorageAccount;
    private final String wasbEndPoint;

    public static SnowflakeStageAccessInfo forS3(String s3AwsAccessKey, String s3AwsSecretKey, String s3AwsSessionToken, String queryStageMasterKey)
    {
        return new SnowflakeStageAccessInfo(StageType.S3, s3AwsAccessKey, s3AwsSecretKey, s3AwsSessionToken, queryStageMasterKey, null, null, null, null);
    }

    public static SnowflakeStageAccessInfo forAzure(String wasbContainer, String wasbSasKey, String wasbStorageAccount, String wasbEndPoint, String queryStageMasterKey)
    {
        return new SnowflakeStageAccessInfo(StageType.AZURE, null, null, null, queryStageMasterKey, wasbContainer, wasbSasKey, wasbStorageAccount, wasbEndPoint);
    }

    // For deserialization only
    @JsonCreator
    public SnowflakeStageAccessInfo(
            @JsonProperty(value = "stageType", required = true) StageType stageType,
            @JsonProperty("s3AwsAccessKey") String s3AwsAccessKey,
            @JsonProperty("s3AwsSecretKey") String s3AwsSecretKey,
            @JsonProperty("s3AwsSessionToken") String s3AwsSessionToken,
            @JsonProperty("queryStageMasterKey") String queryStageMasterKey,
            @JsonProperty("wasbContainer") String wasbContainer,
            @JsonProperty("wasbSasKey") String wasbSasKey,
            @JsonProperty("wasbStorageAccount") String wasbStorageAccount,
            @JsonProperty("wasbEndPoint") String wasbEndPoint)
    {
        this.stageType = requireNonNull(stageType, "stageType is null");
        this.s3AwsAccessKey = s3AwsAccessKey;
        this.s3AwsSecretKey = s3AwsSecretKey;
        this.s3AwsSessionToken = s3AwsSessionToken;
        this.queryStageMasterKey = queryStageMasterKey;
        this.wasbContainer = wasbContainer;
        this.wasbSasKey = wasbSasKey;
        this.wasbStorageAccount = wasbStorageAccount;
        this.wasbEndPoint = wasbEndPoint;
    }

    @JsonProperty
    public StageType getStageType()
    {
        return stageType;
    }

    @JsonProperty
    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @JsonProperty
    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @JsonProperty
    public String getS3AwsSessionToken()
    {
        return s3AwsSessionToken;
    }

    @JsonProperty
    public String getQueryStageMasterKey()
    {
        return queryStageMasterKey;
    }

    @JsonProperty
    public String getWasbContainer()
    {
        return wasbContainer;
    }

    @JsonProperty
    public String getWasbSasKey()
    {
        return wasbSasKey;
    }

    @JsonProperty
    public String getWasbStorageAccount()
    {
        return wasbStorageAccount;
    }

    @JsonProperty
    public String getWasbEndPoint()
    {
        return wasbEndPoint;
    }

    @Override
    public String toString()
    {
        final ToStringHelper helper = toStringHelper(this)
                .add("stageType", this.stageType);
        switch (stageType) {
            case S3:
                helper.add("s3AwsAccessKey", s3AwsAccessKey);
                break;
            case AZURE:
                helper.add("wasbStorageAccount", wasbStorageAccount);
                helper.add("wasbEndPoint", wasbEndPoint);
                break;
            case LOCAL_FS:
            case GCS:
                break;
        }
        return helper.toString();
    }
}
