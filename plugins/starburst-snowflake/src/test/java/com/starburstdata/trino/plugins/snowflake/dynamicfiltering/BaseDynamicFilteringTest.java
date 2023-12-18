/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.dynamicfiltering;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_ENABLED;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;

public abstract class BaseDynamicFilteringTest
        extends AbstractTestQueryFramework
{
    protected Session dynamicFiltering(boolean enabled)
    {
        return dynamicFiltering(getSession(), enabled);
    }

    protected Session dynamicFiltering(Session session, boolean enabled)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().get(), DYNAMIC_FILTERING_ENABLED, Boolean.toString(enabled))
                .setCatalogSessionProperty(getSession().getCatalog().get(), DYNAMIC_FILTERING_WAIT_TIMEOUT, "120s")
                .build();
    }
}
