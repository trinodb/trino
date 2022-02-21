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
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDynamoDbPlugin
{
    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new DynamoDbPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create("test", ImmutableMap.of(), new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("StarburstLicenseException: Valid license required to use the feature: dynamodb");
    }

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingDynamoDbPlugin(false);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("dynamodb.aws-access-key", "access-key")
                        .put("dynamodb.aws-secret-key", "secret-key")
                        .put("dynamodb.aws-region", "us-east-2")
                        .buildOrThrow(),
                new TestingConnectorContext());
    }
}
