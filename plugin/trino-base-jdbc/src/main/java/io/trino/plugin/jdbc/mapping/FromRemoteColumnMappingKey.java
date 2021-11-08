/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.trino.plugin.jdbc.mapping;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FromRemoteColumnMappingKey
{
    private final String remoteSchema;
    private final String remoteTable;
    private final String remoteColumn;

    public FromRemoteColumnMappingKey(
            String remoteSchema,
            String remoteTable,
            String remoteColumn)
    {
        this.remoteSchema = requireNonNull(remoteSchema, "remoteSchema is null");
        this.remoteTable = requireNonNull(remoteTable, "remoteTable is null");
        this.remoteColumn = requireNonNull(remoteColumn, "remoteColumn is null");
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
        FromRemoteColumnMappingKey that = (FromRemoteColumnMappingKey) o;
        return remoteSchema.equals(that.remoteSchema) && remoteTable.equals(that.remoteTable) && remoteColumn.equals(that.remoteColumn);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(remoteSchema, remoteTable, remoteColumn);
    }
}
