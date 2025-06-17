/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradatajdbc;

import io.trino.plugin.jdbc.JdbcPlugin;

public class TeradataPlugin
        extends JdbcPlugin
{
    public TeradataPlugin()
    {
        super("teradatajdbc", TeradataClientModule::new);
    }
}
