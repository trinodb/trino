/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.common.base.Splitter;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Objects;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.DATABASE_SEPARATOR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

public class DatabaseSchemaName
{
    private static final Splitter DATABASE_SPLITTER = Splitter.on(DATABASE_SEPARATOR);

    private final String databaseName;
    private final String schemaName;

    public static DatabaseSchemaName parseDatabaseSchemaName(String schemaName)
    {
        List<String> databaseSchemaName = DATABASE_SPLITTER.splitToList(schemaName);
        if (databaseSchemaName.size() < 2) {
            throw new TrinoException(INVALID_ARGUMENTS, "The expected format is '<database name>.<schema name>': " + schemaName);
        }
        if (databaseSchemaName.size() > 2) {
            throw new TrinoException(INVALID_ARGUMENTS, "Too many identifier parts found");
        }
        return new DatabaseSchemaName(databaseSchemaName.get(0), databaseSchemaName.get(1));
    }

    private DatabaseSchemaName(String databaseName, String schemaName)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public String toString()
    {
        return databaseName + "." + schemaName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseSchemaName that = (DatabaseSchemaName) o;
        return Objects.equals(databaseName, that.databaseName) &&
                Objects.equals(schemaName, that.schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, schemaName);
    }
}
