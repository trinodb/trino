/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.SyntheticColumnHandleBuilder;

import java.util.Set;

public class StargateMetadata
        extends DefaultJdbcMetadata
{
    public StargateMetadata(JdbcClient jdbcClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners, SyntheticColumnHandleBuilder syntheticColumnBuilder)
    {
        // Disable precalculate statistics for pushdown because remote cluster is very good at estimates, should not be inferior to ours.
        super(jdbcClient, false, jdbcQueryEventListeners, syntheticColumnBuilder);
    }
}
