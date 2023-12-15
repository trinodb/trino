/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import com.google.inject.Binder;
import com.google.inject.Module;

import static io.trino.plugin.jdbc.JdbcModule.bindProcedure;

public class TestingSynapseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindProcedure(binder, FlushStatisticsCacheProcedure.class);
    }
}
