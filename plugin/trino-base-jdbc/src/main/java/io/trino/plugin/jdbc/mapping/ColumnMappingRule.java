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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ColumnMappingRule
{
    private final String remoteSchema;
    private final String remoteTable;
    private final String remoteColumn;
    private final String mapping;

    @JsonCreator
    public ColumnMappingRule(
            @JsonProperty String remoteSchema,
            @JsonProperty String remoteTable,
            @JsonProperty String remoteColumn,
            @JsonProperty String mapping)
    {
        this.remoteSchema = requireNonNull(remoteSchema, "remoteSchema is null");
        this.remoteTable = requireNonNull(remoteTable, "remoteTable is null");
        this.remoteColumn = requireNonNull(remoteColumn, "remoteColumn is null");
        this.mapping = requireNonNull(mapping, "mapping is null");
        checkArgument(mapping.toLowerCase(ENGLISH).equals(mapping), "Mapping is not lower cased: %s", mapping);
    }

    @JsonProperty
    public String getRemoteSchema()
    {
        return remoteSchema;
    }

    @JsonProperty
    public String getRemoteTable()
    {
        return remoteTable;
    }

    @JsonProperty
    public String getRemoteColumn()
    {
        return remoteColumn;
    }

    @JsonProperty
    public String getMapping()
    {
        return mapping;
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
        ColumnMappingRule that = (ColumnMappingRule) o;
        return remoteSchema.equals(that.remoteSchema) && remoteTable.equals(that.remoteTable) && remoteColumn.equals(that.remoteColumn) && mapping.equals(that.mapping);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(remoteSchema, remoteTable, remoteColumn, mapping);
    }
}
