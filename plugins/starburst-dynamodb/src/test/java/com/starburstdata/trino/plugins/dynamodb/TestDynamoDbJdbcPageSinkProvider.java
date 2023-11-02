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

import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import org.testng.annotations.Test;

import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestDynamoDbJdbcPageSinkProvider
{
    @Test
    public void testEverythingDelegated()
    {
        assertAllMethodsOverridden(JdbcPageSinkProvider.class, DynamoDbJdbcPageSinkProvider.class);
    }
}
