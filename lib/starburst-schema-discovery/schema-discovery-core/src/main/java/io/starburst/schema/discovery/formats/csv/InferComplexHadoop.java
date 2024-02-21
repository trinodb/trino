/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.csv;

import io.starburst.schema.discovery.infer.NullType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;

import java.util.function.BiFunction;

class InferComplexHadoop
{
    private static final char BASE_ARRAY_DELIMETER = '\002';
    private static final char BASE_MAP_DELIMETER = '\003';
    private static final int MAX_LEVEL = 4;

    // see: http://hadooptutorial.info/hive-data-types-examples/#Complex_Data_Types
    // see: https://stackoverflow.com/a/18828988/2048051
    static TypeInfo inferComplexHadoop(CsvOptions options, TypeInfo typeSoFar, String field, BiFunction<TypeInfo, String, TypeInfo> inferProc)
    {
        if ((typeSoFar instanceof ListTypeInfo) || (typeSoFar instanceof MapTypeInfo)) {
            return typeSoFar;   // punt for now - it will be too complicated to try to widen or average array/map types
        }
        return internalInferComplexHadoop(options, typeSoFar, field, 0, inferProc);
    }

    private static TypeInfo internalInferComplexHadoop(CsvOptions options, TypeInfo typeSoFar, String field, int level, BiFunction<TypeInfo, String, TypeInfo> inferProc)
    {
        if (level <= MAX_LEVEL) {
            if (field.contains(arrayDelimiter(level))) {
                return inferComplexHadoopArray(options, field, level, inferProc);
            }
            if (field.contains(mapDelimiter(level))) {
                return inferComplexHadoopMap(options, field, level, inferProc);
            }
        }
        return inferProc.apply(typeSoFar, field);
    }

    private static TypeInfo inferComplexHadoopArray(CsvOptions options, String field, int level, BiFunction<TypeInfo, String, TypeInfo> inferProc)
    {
        String[] values = field.split(arrayDelimiter(level));
        TypeInfo typeSoFar = NullType.NULL_TYPE;
        for (String value : values) {
            if (value.contains(mapDelimiter(level))) {
                typeSoFar = inferComplexHadoopMap(options, value, level, inferProc);
            }
            else {
                typeSoFar = internalInferComplexHadoop(options, typeSoFar, value, level + 1, inferProc);
            }
        }
        return HiveTypes.arrayType(typeSoFar);
    }

    private static TypeInfo inferComplexHadoopMap(CsvOptions options, String field, int level, BiFunction<TypeInfo, String, TypeInfo> inferProc)
    {
        String[] values = field.split(mapDelimiter(level));
        TypeInfo fieldTypeSoFar = NullType.NULL_TYPE;
        TypeInfo valueTypeSoFar = NullType.NULL_TYPE;
        for (int i = 0; i < values.length; i += 2) {
            fieldTypeSoFar = internalInferComplexHadoop(options, fieldTypeSoFar, values[i], level + 1, inferProc);
            if ((i + 1) < values.length) {
                String value = values[i + 1];
                if (value.contains(arrayDelimiter(level))) {
                    valueTypeSoFar = inferComplexHadoopArray(options, value, level, inferProc);
                }
                else {
                    valueTypeSoFar = internalInferComplexHadoop(options, valueTypeSoFar, value, level + 1, inferProc);
                }
            }
        }
        TypeInfo fieldType = NullType.fixType(fieldTypeSoFar);
        TypeInfo valueType = NullType.fixType(valueTypeSoFar);
        return HiveTypes.mapType(fieldType, valueType);
    }

    private static String arrayDelimiter(int level)
    {
        return Character.toString(BASE_ARRAY_DELIMETER + (2 * level));
    }

    private static String mapDelimiter(int level)
    {
        return Character.toString(BASE_MAP_DELIMETER + (2 * level));
    }

    private InferComplexHadoop()
    {
    }
}
