/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectionFactory.AWS_REGION_TO_CDATA_REGION;
import static java.util.stream.Collectors.joining;

public class DynamoDbConfig
{
    public enum GenerateSchemaFiles
    {
        NEVER("Never"),
        ON_USE("OnUse"),
        ON_START("OnStart"),
        /**/;

        private final String jdbcPropertyValue;

        GenerateSchemaFiles(String jdbcPropertyValue)
        {
            this.jdbcPropertyValue = jdbcPropertyValue;
        }

        public String getJdbcPropertyValue()
        {
            return jdbcPropertyValue;
        }
    }

    private String awsAccessKey;
    private String awsSecretKey;
    private String awsRoleArn;
    private String awsExternalId;
    private String awsRegion;
    private GenerateSchemaFiles generateSchemaFiles = GenerateSchemaFiles.NEVER;
    private String schemaDirectory = JAVA_IO_TMPDIR.value() + "/dynamodb-schemas";
    private boolean flattenObjectsEnabled;
    private int flattenArrayElementCount;
    private boolean isFirstColumnAsPrimaryKeyEnabled;
    private boolean isPredicatePushdownEnabled;
    private String endpointUrl;
    private boolean isDriverLoggingEnabled;
    private String driverLoggingLocation = JAVA_IO_TMPDIR.value() + "/dynamodb.log";
    private int driverLoggingVerbosity = 3;
    private Optional<String> extraJdbcProperties = Optional.empty();

    public Optional<String> getAwsAccessKey()
    {
        return Optional.ofNullable(awsAccessKey);
    }

    @Config("dynamodb.aws-access-key")
    @ConfigDescription("AWS Access Key")
    @ConfigSecuritySensitive
    public DynamoDbConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    public Optional<String> getAwsSecretKey()
    {
        return Optional.ofNullable(awsSecretKey);
    }

    @Config("dynamodb.aws-secret-key")
    @ConfigDescription("AWS Secret Key")
    @ConfigSecuritySensitive
    public DynamoDbConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    public Optional<String> getAwsRoleArn()
    {
        return Optional.ofNullable(awsRoleArn);
    }

    @Config("dynamodb.aws-role-arn")
    @ConfigDescription("AWS Role ARN")
    public DynamoDbConfig setAwsRoleArn(String awsRoleArn)
    {
        this.awsRoleArn = awsRoleArn;
        return this;
    }

    public Optional<String> getAwsExternalId()
    {
        return Optional.ofNullable(awsExternalId);
    }

    @Config("dynamodb.aws-external-id")
    @ConfigDescription("AWS External ID")
    public DynamoDbConfig setAwsExternalId(String awsExternalId)
    {
        this.awsExternalId = awsExternalId;
        return this;
    }

    @NotNull
    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("dynamodb.aws-region")
    @ConfigDescription("AWS Region")
    public DynamoDbConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    @NotNull
    public GenerateSchemaFiles getGenerateSchemaFiles()
    {
        return generateSchemaFiles;
    }

    @Config("dynamodb.generate-schema-files")
    @ConfigDescription("Property directing the connector on whether or not to generate a schema file. Valid values are Never, OnUse, or OnStart. Default Never. See the connector documentation for details.")
    public DynamoDbConfig setGenerateSchemaFiles(GenerateSchemaFiles generateSchemaFiles)
    {
        this.generateSchemaFiles = generateSchemaFiles;
        return this;
    }

    @FileExists
    public String getSchemaDirectory()
    {
        return schemaDirectory;
    }

    @Config("dynamodb.schema-directory")
    @ConfigDescription("Directory containing schema RSD files. The directory will be created if it does not exist. Default ${java.io.tmpdir}/dynamodb-schemas")
    public DynamoDbConfig setSchemaDirectory(String schemaDirectory)
    {
        this.schemaDirectory = schemaDirectory;
        return this;
    }

    public boolean isFlattenObjectsEnabled()
    {
        return flattenObjectsEnabled;
    }

    @Config("dynamodb.flatten-objects-enabled")
    @ConfigDescription("True to enable flattening objects into individual columns. Default false. Also toggleable via session property")
    public DynamoDbConfig setFlattenObjectsEnabled(boolean flattenObjectsEnabled)
    {
        this.flattenObjectsEnabled = flattenObjectsEnabled;
        return this;
    }

    @Min(0)
    @Max(256)
    public int getFlattenArrayElementCount()
    {
        return flattenArrayElementCount;
    }

    @Config("dynamodb.flatten-array-element-count")
    @ConfigDescription("Integer value indicating the number of elements to flatten string set types into. Default is 0 which returns the array as JSON")
    public DynamoDbConfig setFlattenArrayElementCount(int flattenArrayElementCount)
    {
        this.flattenArrayElementCount = flattenArrayElementCount;
        return this;
    }

    public boolean isFirstColumnAsPrimaryKeyEnabled()
    {
        return isFirstColumnAsPrimaryKeyEnabled;
    }

    @Config("dynamodb.first-column-as-primary-key-enabled")
    @ConfigDescription("True to use the first column in the table definition as the primary key if the table property is not set. Default false.")
    public DynamoDbConfig setFirstColumnAsPrimaryKeyEnabled(boolean isFirstColumnAsPrimaryKeyEnabled)
    {
        this.isFirstColumnAsPrimaryKeyEnabled = isFirstColumnAsPrimaryKeyEnabled;
        return this;
    }

    public boolean isPredicatePushdownEnabled()
    {
        return isPredicatePushdownEnabled;
    }

    @Config("dynamodb.predicate-pushdown-enabled")
    @ConfigDescription("True to enable partial predicate pushdown. Default false.")
    public DynamoDbConfig setPredicatePushdownEnabled(boolean isPredicatePushdownEnabled)
    {
        this.isPredicatePushdownEnabled = isPredicatePushdownEnabled;
        return this;
    }

    public Optional<String> getEndpointUrl()
    {
        return Optional.ofNullable(endpointUrl);
    }

    @Config("dynamodb.endpoint-url")
    @ConfigDescription("Endpoint URL override for testing with Local DynamoDB. Users should not need to set this.")
    public DynamoDbConfig setEndpointUrl(String endpointUrl)
    {
        this.endpointUrl = endpointUrl;
        return this;
    }

    public boolean isDriverLoggingEnabled()
    {
        return isDriverLoggingEnabled;
    }

    @Config("dynamodb.driver-logging.enabled")
    @ConfigDescription("True to enable the logging feature on the driver. Default is false")
    public DynamoDbConfig setDriverLoggingEnabled(boolean isDriverLoggingEnabled)
    {
        this.isDriverLoggingEnabled = isDriverLoggingEnabled;
        return this;
    }

    public String getDriverLoggingLocation()
    {
        return driverLoggingLocation;
    }

    @Config("dynamodb.driver-logging.location")
    @ConfigDescription("Sets the name of the file for to put driver logs. Default is ${java.io.tmpdir}/dynamodb.log")
    public DynamoDbConfig setDriverLoggingLocation(String driverLoggingLocation)
    {
        this.driverLoggingLocation = driverLoggingLocation;
        return this;
    }

    @Min(1)
    @Max(5)
    public int getDriverLoggingVerbosity()
    {
        return driverLoggingVerbosity;
    }

    @Config("dynamodb.driver-logging.verbosity")
    @ConfigDescription("Sets the logging verbosity level. Default level 3")
    public DynamoDbConfig setDriverLoggingVerbosity(int driverLoggingVerbosity)
    {
        this.driverLoggingVerbosity = driverLoggingVerbosity;
        return this;
    }

    public Optional<String> getExtraJdbcProperties()
    {
        return extraJdbcProperties;
    }

    @Config("dynamodb.extra-jdbc-properties")
    @ConfigDescription("Any extra properties to add to the JDBC Driver connection")
    public DynamoDbConfig setExtraJdbcProperties(String extraJdbcProperties)
    {
        this.extraJdbcProperties = Optional.ofNullable(extraJdbcProperties);
        return this;
    }

    @PostConstruct
    public void validate()
    {
        checkState(AWS_REGION_TO_CDATA_REGION.containsKey(getAwsRegion()), "dynamodb.aws-region must be one of the following: " + AWS_REGION_TO_CDATA_REGION.keySet().stream().sorted().collect(joining(", ")));
        checkState(getAwsAccessKey().isPresent() == getAwsSecretKey().isPresent(), "dynamodb.aws-access-key and dynamodb.aws-secret-key must both be either set or not set");
    }
}
