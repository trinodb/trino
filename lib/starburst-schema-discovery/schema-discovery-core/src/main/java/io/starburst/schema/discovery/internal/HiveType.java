/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.plugin.hive.type.TypeInfo;

import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeInfoFromTypeString;
import static java.util.Objects.requireNonNull;

public record HiveType(TypeInfo typeInfo)
{
    public HiveType
    {
        requireNonNull(typeInfo, "typeInfo is null");
    }

    @JsonValue
    public String value()
    {
        return typeInfo.toString();
    }

    @JsonCreator
    public static HiveType valueOf(String hiveTypeName)
    {
        requireNonNull(hiveTypeName, "hiveTypeName is null");
        return new HiveType(getTypeInfoFromTypeString(hiveTypeName));
    }
}
