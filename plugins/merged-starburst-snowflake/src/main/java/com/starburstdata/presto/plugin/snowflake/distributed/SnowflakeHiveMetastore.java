/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.trino.plugin.hive.metastore.Table;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

class SnowflakeHiveMetastore
        extends UnimplementedHiveMetastore
{
    private final Table table;

    SnowflakeHiveMetastore(Table table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        checkArgument(table.getDatabaseName().toLowerCase(ENGLISH).equals(databaseName), "databaseName does not match");
        checkArgument(table.getTableName().toLowerCase(ENGLISH).equals(tableName), "tableName does not match");

        return Optional.of(table);
    }
}
