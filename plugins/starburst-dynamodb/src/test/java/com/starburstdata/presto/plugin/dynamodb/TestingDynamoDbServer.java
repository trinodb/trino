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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public class TestingDynamoDbServer
        implements AutoCloseable
{
    private static final int PORT = 8000;

    private final GenericContainer<?> dockerContainer;
    private final File schemaDirectory;

    public TestingDynamoDbServer()
    {
        dockerContainer = new GenericContainer<>("amazon/dynamodb-local")
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
    }

    public String getEndpointUrl()
    {
        return "http://localhost:" + dockerContainer.getMappedPort(PORT);
    }

    public File getSchemaDirectory()
    {
        return schemaDirectory;
    }

    @Override
    public void close()
            throws IOException
    {
        dockerContainer.close();
        deleteDirectory(schemaDirectory);
    }
}
