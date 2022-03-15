/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.trino.testing.sql.SqlExecutor;

import java.util.Locale;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestDatabase
        implements AutoCloseable
{
    private final SqlExecutor sqlExecutor;
    private final String name;

    public TestDatabase(SqlExecutor sqlExecutor, String namePrefix)
    {
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.name = format("%s_%s", namePrefix, randomTableSuffix()).toUpperCase(Locale.ENGLISH);
        sqlExecutor.execute(format("CREATE DATABASE IF NOT EXISTS %s", name));
    }

    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP DATABASE IF EXISTS " + name);
    }
}
