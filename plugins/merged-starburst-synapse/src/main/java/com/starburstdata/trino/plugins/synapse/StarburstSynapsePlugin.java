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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.trino.plugin.jdbc.JdbcPlugin;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class StarburstSynapsePlugin
        extends JdbcPlugin
{
    public StarburstSynapsePlugin()
    {
        super("synapse", new StarburstSynapseClientModule());
    }

    @VisibleForTesting
    StarburstSynapsePlugin(Module testingExtensions)
    {
        super("synapse", combine(new StarburstSynapseClientModule(), testingExtensions));
    }
}
