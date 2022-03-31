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

import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;

public class StargateMetadata
        extends DefaultJdbcMetadata
{
    public StargateMetadata(JdbcClient jdbcClient)
    {
        // Disable precalculate statistics for pushdown because remote cluster is very good at estimates, should not be inferior to ours.
        super(jdbcClient, false);
    }
}
