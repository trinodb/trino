/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbSessionProperties.getFlattenArrayElementCount;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbSessionProperties.getGenerateSchemaFiles;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbSessionProperties.isFlattenObjectsEnabled;

/**
 * We implement our own ConnectionFactory in order to add our OEM key to the connection URL but prevent it from being logged
 * CData does not want our customers to know what the OEM key is as it will unlock all their drivers
 */
public class DynamoDbConnectionFactory
        implements ConnectionFactory
{
    public static final Map<String, String> AWS_REGION_TO_CDATA_REGION;

    private static final String DRIVER_CLASS_NAME = "cdata.jdbc.amazondynamodb.AmazonDynamoDBDriver";
    private static final String CDATA_OEM_KEY = "StarburstData_6b415f4923004c64ac4da9223bfc053f_v1.0";

    static {
        // The following regions are supported by the CData driver
        // The driver does not accept a standard AWS name but instead uses the location of the region
        // These inputs are validated in DynamoDbConfig
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("us-east-2", "OHIO");
        builder.put("us-east-1", "NORTHERNVIRGINIA");
        builder.put("us-west-1", "NORTHERNCALIFORNIA");
        builder.put("us-west-2", "OREGON");
        builder.put("af-south-1", "CAPETOWN");
        builder.put("ap-east-1", "HONGKONG");
        builder.put("ap-south-1", "MUMBAI");
        builder.put("ap-northeast-3", "OSAKA");
        builder.put("ap-northeast-2", "SEOUL");
        builder.put("ap-southeast-1", "SINGAPORE");
        builder.put("ap-southeast-2", "SYDNEY");
        builder.put("ap-northeast-1", "TOKYO");
        builder.put("ca-central-1", "CENTRAL");
        builder.put("cn-north-1", "BEIJING");
        builder.put("us-iso-east-1", "NINGXIA");
        builder.put("eu-central-1", "FRANKFURT");
        builder.put("eu-west-1", "IRELAND");
        builder.put("eu-west-2", "LONDON");
        builder.put("eu-south-1", "MILAN");
        builder.put("eu-west-3", "PARIS");
        builder.put("eu-north-1", "STOCKHOLM");
        builder.put("me-south-1", "BAHRAIN");
        builder.put("sa-east-1", "SAOPAULO");
        builder.put("us-gov-east-1", "GOVCLOUDEAST");
        builder.put("us-gov-west-1", "GOVCLOUDWEST");
        AWS_REGION_TO_CDATA_REGION = builder.build();
    }

    public static String getConnectionUrl(DynamoDbConfig dynamoDbConfig)
    {
        checkArgument(AWS_REGION_TO_CDATA_REGION.containsKey(dynamoDbConfig.getAwsRegion()), "No mapping of AWS region to location for value: " + dynamoDbConfig.getAwsRegion());

        StringBuilder builder = new StringBuilder("jdbc:amazondynamodb:")
                .append("AWS Region=\"").append(AWS_REGION_TO_CDATA_REGION.get(dynamoDbConfig.getAwsRegion())).append("\";")
                .append("IgnoreTypes=\"Datetime,Time\";") // Change default of IgnoreTypes to support date types
                .append("UseBatchWriteItemOperation=\"True\";") // Enable BatchWriteItemOperation to support varbinary types
                .append("ReportMetadataExceptions=\"True\";") // Throw exceptions on failing metadata operations
                .append("OEMKey=\"").append(CDATA_OEM_KEY).append("\";");

        // Both of these settings are validated in DynamoDbConfig
        if (dynamoDbConfig.getAwsAccessKey().isPresent() && dynamoDbConfig.getAwsSecretKey().isPresent()) {
            builder.append("AWS Access Key=\"").append(dynamoDbConfig.getAwsAccessKey().get()).append("\";");
            builder.append("AWS Secret Key=\"").append(dynamoDbConfig.getAwsSecretKey().get()).append("\";");
        }
        else {
            // If they are not set, set auth scheme to EC2 roles so driver does not throw an error
            builder.append("Auth Scheme=\"AwsEC2Roles\";");
        }

        dynamoDbConfig.getAwsRoleArn().ifPresent(url -> builder.append("AWS Role ARN=\"").append(url).append("\";"));
        dynamoDbConfig.getAwsExternalId().ifPresent(url -> builder.append("AWS External Id=\"").append(url).append("\";"));
        dynamoDbConfig.getEndpointUrl().ifPresent(url -> builder.append("URL=\"").append(url).append("\";"));

        if (dynamoDbConfig.isDriverLoggingEnabled()) {
            builder.append("LogFile=\"").append(dynamoDbConfig.getDriverLoggingLocation()).append("\";")
                    .append("Verbosity=\"").append(dynamoDbConfig.getDriverLoggingVerbosity()).append("\";");
        }

        Optional.ofNullable(dynamoDbConfig.getSchemaDirectory()).ifPresent(directory -> builder.append("Location=\"").append(directory).append("\";"));
        dynamoDbConfig.getExtraJdbcProperties().ifPresent(builder::append);

        return builder.toString();
    }

    // We use Driver here rather than delegating to another ConnectionFactory in order to prevent
    // the URL from being logged or thrown in an error message
    private final Driver driver;
    private final String connectionUrl;
    private final CredentialPropertiesProvider<String, String> credentialPropertiesProvider;

    public DynamoDbConnectionFactory(DynamoDbConfig dynamoDbConfig, CredentialProvider credentialProvider)
    {
        Class<? extends Driver> driverClass;
        try {
            driverClass = Class.forName(DRIVER_CLASS_NAME).asSubclass(Driver.class);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw new RuntimeException("Failed to load Driver class: " + DRIVER_CLASS_NAME, e);
        }

        try {
            driver = driverClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create instance of Driver: " + DRIVER_CLASS_NAME, e);
        }

        connectionUrl = getConnectionUrl(dynamoDbConfig);
        credentialPropertiesProvider = new DefaultCredentialPropertiesProvider(credentialProvider);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Properties properties = getCredentialProperties(session.getIdentity());

        String url = connectionUrl +
                "GenerateSchemaFiles=\"" + getGenerateSchemaFiles(session).getJdbcPropertyValue() + "\";" +
                "FlattenObjects=\"" + isFlattenObjectsEnabled(session) + "\";";
        if (getFlattenArrayElementCount(session) > 0) {
            url = url + "FlattenArrays=\"" + getFlattenArrayElementCount(session) + "\";";
        }

        Connection connection = driver.connect(url, properties);
        // Remove OEM key from log
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", url.replaceAll("OEMKey=\".+?\";", ""), driver);
        return connection;
    }

    private Properties getCredentialProperties(ConnectorIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }
}
