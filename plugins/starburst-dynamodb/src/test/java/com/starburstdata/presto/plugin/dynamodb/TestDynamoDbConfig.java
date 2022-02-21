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
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbConfig.GenerateSchemaFiles.NEVER;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbConfig.GenerateSchemaFiles.ON_USE;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDynamoDbConfig
{
    @Test
    public void testDefaults()
            throws IOException
    {
        // Create directory which must exist
        Files.createDirectories(Path.of(JAVA_IO_TMPDIR.value(), "dynamodb-schemas"));

        assertRecordedDefaults(recordDefaults(DynamoDbConfig.class)
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setAwsRoleArn(null)
                .setAwsExternalId(null)
                .setAwsRegion(null)
                .setGenerateSchemaFiles(NEVER)
                .setSchemaDirectory(JAVA_IO_TMPDIR.value() + "/dynamodb-schemas")
                .setFirstColumnAsPrimaryKeyEnabled(false)
                .setFlattenObjectsEnabled(false)
                .setFlattenArrayElementCount(0)
                .setEndpointUrl(null)
                .setDriverLoggingEnabled(false)
                .setDriverLoggingLocation(JAVA_IO_TMPDIR.value() + "/dynamodb.log")
                .setDriverLoggingVerbosity(3)
                .setExtraJdbcProperties(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        // Create non-default temp directory which must exist
        File tempDirectory = Files.createTempDirectory("dynamodb").toFile();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("dynamodb.aws-access-key", "access-key")
                .put("dynamodb.aws-secret-key", "secret-key")
                .put("dynamodb.aws-role-arn", "role-arn")
                .put("dynamodb.aws-external-id", "external-id")
                .put("dynamodb.aws-region", "us-east-2")
                .put("dynamodb.generate-schema-files", "ON_USE")
                .put("dynamodb.schema-directory", tempDirectory.getAbsolutePath())
                .put("dynamodb.first-column-as-primary-key-enabled", "true")
                .put("dynamodb.flatten-objects-enabled", "true")
                .put("dynamodb.flatten-array-element-count", "3")
                .put("dynamodb.endpoint-url", "http://localhost:8000")
                .put("dynamodb.driver-logging.enabled", "true")
                .put("dynamodb.driver-logging.location", "/tmp/bar")
                .put("dynamodb.driver-logging.verbosity", "5")
                .put("dynamodb.extra-jdbc-properties", "foo=bar;")
                .build();

        DynamoDbConfig expected = new DynamoDbConfig()
                .setAwsAccessKey("access-key")
                .setAwsSecretKey("secret-key")
                .setAwsRoleArn("role-arn")
                .setAwsExternalId("external-id")
                .setAwsRegion("us-east-2")
                .setGenerateSchemaFiles(ON_USE)
                .setSchemaDirectory(tempDirectory.getAbsolutePath())
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .setFlattenObjectsEnabled(true)
                .setFlattenArrayElementCount(3)
                .setEndpointUrl("http://localhost:8000")
                .setDriverLoggingEnabled(true)
                .setDriverLoggingLocation("/tmp/bar")
                .setDriverLoggingVerbosity(5)
                .setExtraJdbcProperties("foo=bar;");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testIncorrectRegion()
    {
        assertThatThrownBy(() -> new DynamoDbConfig().setAwsRegion("foobar").validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("dynamodb.aws-region must be one of the following: af-south-1, ap-east-1, ap-northeast-1, ap-northeast-2, ap-northeast-3, ap-south-1, ap-southeast-1, ap-southeast-2, ca-central-1, cn-north-1, eu-central-1, eu-north-1, eu-south-1, eu-west-1, eu-west-2, eu-west-3, me-south-1, sa-east-1, us-east-1, us-east-2, us-gov-east-1, us-gov-west-1, us-iso-east-1, us-west-1, us-west-2");
    }

    @Test
    public void testAwsKeysNotBothSet()
    {
        // Test both not set
        new DynamoDbConfig().setAwsRegion("us-east-2").validate();

        // Test only one set throws an error
        assertThatThrownBy(() -> new DynamoDbConfig().setAwsRegion("us-east-2").setAwsAccessKey("foo").validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("dynamodb.aws-access-key and dynamodb.aws-secret-key must both be either set or not set");
        assertThatThrownBy(() -> new DynamoDbConfig().setAwsRegion("us-east-2").setAwsSecretKey("bar").validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("dynamodb.aws-access-key and dynamodb.aws-secret-key must both be either set or not set");

        // Test both set
        new DynamoDbConfig()
                .setAwsRegion("us-east-2")
                .setAwsAccessKey("foo")
                .setAwsSecretKey("bar")
                .validate();
    }

    @Test
    public void testRegionValues()
            throws Exception
    {
        DynamoDbConfig config = new DynamoDbConfig()
                .setAwsAccessKey("access-key")
                .setAwsSecretKey("secret-key")
                .setAwsRegion("us-east-2");

        // Query the CData driver to get the list of supported AWS regions
        // Assert that we have all of them in our own AWS region mapping of the AWS name to the CData location
        try (Connection connection = DriverManager.getConnection(DynamoDbConnectionFactory.getConnectionUrl(config));
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT * FROM sys_connection_props WHERE Name = 'Aws Region'")) {
            assertTrue(results.next());
            Set<String> actual = Arrays.stream(results.getString("Values").split(",")).collect(toSet());
            Set<String> expected = new HashSet<>(DynamoDbConnectionFactory.AWS_REGION_TO_CDATA_REGION.values());
            assertEquals(actual, expected);
        }
    }
}
