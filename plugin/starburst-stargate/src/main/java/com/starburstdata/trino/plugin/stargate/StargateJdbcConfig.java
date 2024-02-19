/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.plugin.jdbc.BaseJdbcConfig;

import static com.starburstdata.trino.plugin.stargate.StarburstJdbcUrl.Presence.PRESENT;

public class StargateJdbcConfig
        extends BaseJdbcConfig
{
    // TODO Cannot override getConnectionUrl as Airlift complains about multiple getters for a property
    @StarburstJdbcUrl(catalog = PRESENT)
    public String getStarburstConnectionUrl()
    {
        return super.getConnectionUrl();
    }
}
