/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

public enum SnowflakeConnectorFlavour
{
    JDBC("snowflake_jdbc"),
    DISTRIBUTED("snowflake_distributed"),
    PARALLEL("snowflake_parallel");

    private final String name;

    SnowflakeConnectorFlavour(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }
}
