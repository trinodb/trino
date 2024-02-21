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

import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.type.Type;

public interface SqlType
{
    String getSqlType();

    static SqlType sqlType(String from)
    {
        return () -> from;
    }

    static SqlType sqlType(TypeInfo typeInfo)
    {
        return typeInfo::getTypeName;
    }

    static SqlType sqlType(Type type)
    {
        return type::getDisplayName;
    }
}
