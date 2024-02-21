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

import io.trino.plugin.hive.type.MapTypeInfo;

import static java.util.Objects.requireNonNull;

class InternalMapTypeInfo
        implements SqlType
{
    private final MapTypeInfo map;

    InternalMapTypeInfo(MapTypeInfo map)
    {
        this.map = requireNonNull(map, "map is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o instanceof InternalMapTypeInfo internal) {
            return map.equals(internal.map);
        }
        if (o instanceof MapTypeInfo map) {
            return this.map.equals(map);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return map.hashCode();
    }

    @Override
    public String getSqlType()
    {
        return "map(" + Mapping.convert(map.getMapKeyTypeInfo()).getSqlType() + "," + Mapping.convert(map.getMapValueTypeInfo()).getSqlType() + ")";
    }

    @Override
    public String toString()
    {
        return getSqlType();
    }
}
