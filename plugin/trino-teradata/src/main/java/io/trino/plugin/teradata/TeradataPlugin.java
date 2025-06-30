/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.JdbcPlugin;

/**
 * Teradata plugin for Trino.
 * <p>
 * This class registers the Teradata connector plugin with Trino,
 * extending the base {@link JdbcPlugin} class.
 * It provides the connector name ("teradata") and the client module
 * to use for Teradata JDBC connectivity.
 * </p>
 */
public class TeradataPlugin
        extends JdbcPlugin
{
    /**
     * Constructs a new TeradataPlugin instance.
     * <p>
     * The plugin is identified by the name "teradata" and
     * uses the {@link TeradataClientModule} to configure
     * the JDBC client for Teradata.
     * </p>
     */
    public TeradataPlugin()
    {
        super("teradata", TeradataClientModule::new);
    }
}
