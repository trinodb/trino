/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.generation;

import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

class InternalStructTypeInfo
        implements SqlType
{
    private final StructTypeInfo struct;

    InternalStructTypeInfo(StructTypeInfo struct)
    {
        this.struct = requireNonNull(struct, "struct is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (o instanceof InternalStructTypeInfo internal) {
            return struct.equals(internal.struct);
        }
        if (o instanceof StructTypeInfo struct) {
            return this.struct.equals(struct);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return struct.hashCode();
    }

    @Override
    public String getSqlType()
    {
        int qty = Math.min(struct.getAllStructFieldNames().size(), struct.getAllStructFieldTypeInfos().size());
        String spec = IntStream.range(0, qty)
                .mapToObj(index -> {
                    String name = struct.getAllStructFieldNames().get(index);
                    TypeInfo typeInfo = struct.getAllStructFieldTypeInfos().get(index);
                    return Mapping.doubleQuote(name) + " " + Mapping.convert(typeInfo).getSqlType();
                })
                .collect(Collectors.joining(","));
        return "row(" + spec + ")";
    }

    @Override
    public String toString()
    {
        return getSqlType();
    }
}
