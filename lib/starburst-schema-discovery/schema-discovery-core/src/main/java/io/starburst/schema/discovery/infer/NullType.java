/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.infer;

import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.type.TypeInfoUtils;
import io.trino.plugin.hive.util.SerdeConstants;

import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;

public class NullType
{
    public static final TypeInfo NULL_TYPE = TypeInfoUtils.getTypeInfoFromTypeString(SerdeConstants.VOID_TYPE_NAME);

    public static boolean isNullType(TypeInfo type)
    {
        return type == NULL_TYPE;
    }

    public static TypeInfo fixType(TypeInfo type)
    {
        return isNullType(type) ? STRING_TYPE : type;
    }

    private NullType()
    {
    }
}
