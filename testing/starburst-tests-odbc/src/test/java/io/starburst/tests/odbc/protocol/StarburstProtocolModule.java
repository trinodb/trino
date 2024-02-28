/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.tests.odbc.protocol;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class StarburstProtocolModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jaxrsBinder(binder).bind(PreparedStatementRequestFilter.class);
        jaxrsBinder(binder).bind(StatementResponseFilter.class);
    }
}
