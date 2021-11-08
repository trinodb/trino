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

public class ToRemoteColumnMappingKey
{
    private final String remoteSchema;
    private final String remoteTable;
    private final String mapping;

    public ToRemoteColumnMappingKey(
            String remoteSchema,
            String remoteTable,
            String mapping)
    {
        this.remoteSchema = requireNonNull(remoteSchema, "remoteSchema is null");
        this.remoteTable = requireNonNull(remoteTable, "remoteTable is null");
        this.mapping = requireNonNull(mapping, "mapping is null");
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
        ToRemoteColumnMappingKey that = (ToRemoteColumnMappingKey) o;
        return remoteSchema.equals(that.remoteSchema) && remoteTable.equals(that.remoteTable) && mapping.equals(that.mapping);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(remoteSchema, remoteTable, mapping);
    }
}
