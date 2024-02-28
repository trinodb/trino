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

import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;

public abstract class StatementRequestFilter
        implements DynamicFeature
{
    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context)
    {
        if ((resourceInfo.getResourceClass().getCanonicalName().equals("io.trino.dispatcher.QueuedStatementResource")
                || resourceInfo.getResourceClass().getCanonicalName().equals("com.starburstdata.presto.insights.StatementResource"))
                && resourceInfo.getResourceMethod().getName().equals("postStatement")) {
            context.register(create());
        }
    }

    protected abstract ContainerRequestFilter create();
}
