/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import io.trino.testing.sql.SqlExecutor;

import java.io.Closeable;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public class TestView
        implements Closeable
{
    private final SqlExecutor sqlExecutor;
    private final String name;

    public TestView(SqlExecutor sqlExecutor, String namePrefix, String definition)
    {
        this.sqlExecutor = sqlExecutor;
        this.name = namePrefix + "_" + randomTableSuffix();
        sqlExecutor.execute(format("CREATE VIEW %s %s", name, definition));
    }

    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP VIEW " + name);
    }
}
