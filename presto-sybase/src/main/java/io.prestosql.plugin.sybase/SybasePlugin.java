package io.prestosql.plugin.sybase;

import io.prestosql.plugin.jdbc.JdbcPlugin;

/**
 * Initial class injected into PrestoSQL via SPI
 */

 public class SybasePlugin extends JdbcPlugin {

    public SybasePlugin() {
        // name of the connector and module implementation
        super("sybase", new SybaseClientModule());
    }
 }
