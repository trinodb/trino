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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestDynamoDbPlugin
{
    @Test
    public void testCreateConnector()
            throws Exception
    {
        File tempDirectory = Files.createTempDirectory("dynamodb-schemas").toFile();
        Plugin plugin = new TestingDynamoDbPlugin(false);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("dynamodb.aws-access-key", "accesskey")
                        .put("dynamodb.aws-secret-key", "secretkey")
                        .put("dynamodb.aws-region", "us-east-2")
                        .put("dynamodb.schema-directory", tempDirectory.getAbsolutePath())
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }
}
