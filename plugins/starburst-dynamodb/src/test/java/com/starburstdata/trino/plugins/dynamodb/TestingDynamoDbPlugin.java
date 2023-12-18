/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamodb;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.trino.plugins.dynamodb.DynamoDbQueryRunner.NOOP_LICENSE_MANAGER;

public class TestingDynamoDbPlugin
        extends DynamoDbPlugin
{
    private final boolean enableWrites;

    public TestingDynamoDbPlugin(boolean enableWrites)
    {
        super(NOOP_LICENSE_MANAGER);
        this.enableWrites = enableWrites;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(getConnectorFactory(enableWrites));
    }
}
