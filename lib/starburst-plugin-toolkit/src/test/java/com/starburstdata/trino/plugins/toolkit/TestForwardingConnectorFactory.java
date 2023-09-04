/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.toolkit;

import io.trino.spi.connector.ConnectorFactory;
import org.testng.annotations.Test;

import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;

public class TestForwardingConnectorFactory
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorFactory.class, ForwardingConnectorFactory.class);
    }

    @Test
    public void testProperForwardingMethodsAreCalled()
    {
        assertProperForwardingMethodsAreCalled(ConnectorFactory.class, delegate -> new ForwardingConnectorFactory()
        {
            @Override
            protected ConnectorFactory delegate()
            {
                return delegate;
            }
        });
    }
}
