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

import io.trino.plugin.hive.type.ListTypeInfo;

import static java.util.Objects.requireNonNull;

class InternalListTypeInfo
        implements SqlType
{
    private final ListTypeInfo list;

    InternalListTypeInfo(ListTypeInfo list)
    {
        this.list = requireNonNull(list, "list is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o instanceof InternalListTypeInfo internal) {
            return list.equals(internal.list);
        }
        if (o instanceof ListTypeInfo list) {
            return this.list.equals(list);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return list.hashCode();
    }

    @Override
    public String getSqlType()
    {
        return "array(" + Mapping.convert(list.getListElementTypeInfo()).getSqlType() + ")";
    }

    @Override
    public String toString()
    {
        return getSqlType();
    }
}
