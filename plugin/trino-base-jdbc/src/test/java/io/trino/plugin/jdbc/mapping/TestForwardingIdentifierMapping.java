/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.trino.plugin.jdbc.mapping;

import org.testng.annotations.Test;

import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;

public class TestForwardingIdentifierMapping
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(IdentifierMapping.class, ForwardingIdentifierMapping.class);
    }

    @Test
    public void testProperForwardingMethodsAreCalled()
    {
        assertProperForwardingMethodsAreCalled(IdentifierMapping.class, identifierMapping -> ForwardingIdentifierMapping.of(() -> identifierMapping));
    }
}
