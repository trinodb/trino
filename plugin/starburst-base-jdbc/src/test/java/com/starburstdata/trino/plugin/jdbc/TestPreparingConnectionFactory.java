/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.jdbc;

import io.trino.plugin.jdbc.ConnectionFactory;
import org.junit.jupiter.api.Test;

import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestPreparingConnectionFactory
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectionFactory.class, PreparingConnectionFactory.class);
    }
}
