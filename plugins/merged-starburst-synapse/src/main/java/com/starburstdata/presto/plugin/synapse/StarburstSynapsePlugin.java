/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import io.trino.plugin.jdbc.JdbcPlugin;

public class StarburstSynapsePlugin
        extends JdbcPlugin
{
    public StarburstSynapsePlugin()
    {
        super("synapse", new StarburstSynapseClientModule());
    }
}
