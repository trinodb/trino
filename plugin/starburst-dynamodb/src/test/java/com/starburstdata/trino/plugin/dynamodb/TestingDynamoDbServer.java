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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public class TestingDynamoDbServer
        implements AutoCloseable
{
    private static final int PORT = 8000;

    private final GenericContainer<?> dockerContainer;
    private final File schemaDirectory;
    private final DynamoDbConfig config;

    public TestingDynamoDbServer()
    {
        dockerContainer = new GenericContainer<>("amazon/dynamodb-local:2.1.0")
                .withExposedPorts(PORT)
                .waitingFor(Wait.forLogMessage(".*Initializing DynamoDB Local with the following configuration.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        dockerContainer.start();

        try {
            schemaDirectory = Files.createTempDirectory("dynamodb-schemas").toFile();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory for schemas", e);
        }
        config = new DynamoDbConfig()
                .setAwsAccessKey("access-key")
                .setAwsSecretKey("secret-key")
                .setAwsRegion("us-east-2")
                .setSchemaDirectory(schemaDirectory.getAbsolutePath())
                .setEndpointUrl(getEndpointUrl());
    }

    public String getEndpointUrl()
    {
        return "http://localhost:" + dockerContainer.getMappedPort(PORT);
    }

    public File getSchemaDirectory()
    {
        return schemaDirectory;
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(DynamoDbConnectionFactory.getConnectionUrl(config));
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        dockerContainer.close();
        deleteDirectory(schemaDirectory);
    }
}
