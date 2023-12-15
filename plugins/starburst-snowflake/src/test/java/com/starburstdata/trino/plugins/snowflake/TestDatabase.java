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

import io.trino.testing.sql.SqlExecutor;

import java.io.Closeable;
import java.util.Locale;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestDatabase
        implements Closeable
{
    private final SqlExecutor sqlExecutor;
    private final String name;

    public static final String tmpDatabasePrefix = "TMP_SEP_CICD_";
    public static final String tmpDatabaseComment = "Created by SEP tests";

    public TestDatabase(SqlExecutor sqlExecutor, String namePrefix)
    {
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.name = format("%s%s_%s", tmpDatabasePrefix, namePrefix, randomNameSuffix()).toUpperCase(Locale.ENGLISH);
        sqlExecutor.execute(format("CREATE DATABASE IF NOT EXISTS %s COMMENT = '%s'", name, tmpDatabaseComment));
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
