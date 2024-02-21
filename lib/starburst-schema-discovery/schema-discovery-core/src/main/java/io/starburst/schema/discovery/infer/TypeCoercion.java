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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.starburst.schema.discovery.SchemaDiscoveryErrorCode;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_FLOAT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_SHORT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE_ALT;
import static io.starburst.schema.discovery.internal.HiveTypes.buildNameToInfoMap;
import static io.starburst.schema.discovery.internal.HiveTypes.isArrayType;
import static io.starburst.schema.discovery.internal.HiveTypes.isMapType;
import static io.starburst.schema.discovery.internal.HiveTypes.isStructType;
import static io.starburst.schema.discovery.internal.HiveTypes.structType;

public class TypeCoercion
{
    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L128
    private static final DecimalTypeInfo ShortDecimal = new DecimalTypeInfo(5, 0);
    private static final DecimalTypeInfo IntDecimal = new DecimalTypeInfo(10, 0);
    private static final DecimalTypeInfo LongDecimal = new DecimalTypeInfo(20, 0);
    private static final DecimalTypeInfo DoubleDecimal = new DecimalTypeInfo(30, 15);

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala#L73
    // NOTE: in our case, this list is reversed
    private static final List<TypeInfo> numericPrecedence = ImmutableList.of(
            HIVE_DOUBLE,
            HIVE_FLOAT,
            HIVE_LONG,
            HIVE_INT,
            HIVE_SHORT);

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L195
    public static Optional<TypeInfo> compatibleType(TypeInfo t1, TypeInfo t2)
    {
        Optional<TypeInfo> tightestCommonType = findTightestCommonType(t1, t2);
        return tightestCommonType.isPresent() ? tightestCommonType : findCompatibleTypeForCSV(t1, t2);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala#L90
    public static Optional<TypeInfo> findTightestCommonType(TypeInfo t1, TypeInfo t2)
    {
        if (t1 == t2) {
            return Optional.of(t1);
        }
        if (NullType.isNullType(t1)) {
            return Optional.of(t2);
        }
        if (NullType.isNullType(t2)) {
            return Optional.of(t1);
        }
        if (isDecimalType(t1) && isDecimalType(t2)) {
            return isWiderThan((DecimalTypeInfo) t1, t2) ?
                    Optional.of(t1) :
                    Optional.of(t2);
        }
        if (isIntegralType(t1) && isDecimalType(t2) && isWiderThan((DecimalTypeInfo) t2, t1)) {
            return Optional.of(t2);
        }
        if (isIntegralType(t2) && isDecimalType(t1) && isWiderThan((DecimalTypeInfo) t1, t2)) {
            return Optional.of(t1);
        }

        // Promote numeric types to the highest of the two
        if (isNumericType(t1) && isNumericType(t2) && !isDecimalType(t1) && !isDecimalType(t2)) {
            return numericPrecedence.stream().filter(t -> (t == t1) || (t == t2)).findFirst();
        }

        if ((isTimestamp(t1) && isDate(t2)) || (isDate(t1) && isTimestamp(t2))) {
            return Optional.of(HIVE_TIMESTAMP);
        }

        return findTypeForComplex(t1, t2);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L203
    static Optional<TypeInfo> findCompatibleTypeForCSV(TypeInfo t1, TypeInfo t2)
    {
        if (isStringType(t1) || isStringType(t2)) {
            return Optional.of(STRING_TYPE);
        }

        // These two cases below deal with when `IntegralType` is larger than `DecimalType`.
        if (isIntegralType(t1) && isDecimalType(t2)) {
            return compatibleType(forType(t1), t2);
        }
        if (isIntegralType(t2) && isDecimalType(t1)) {
            return compatibleType(forType(t2), t1);
        }

        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        if ((isDoubleType(t1) && isDecimalType(t2)) || (isDoubleType(t2) && isDecimalType(t1))) {
            return Optional.of(HIVE_DOUBLE);
        }

        if (isDecimalType(t1) && isDecimalType(t2)) {
            DecimalTypeInfo d1 = (DecimalTypeInfo) t1;
            DecimalTypeInfo d2 = (DecimalTypeInfo) t2;
            int scale = Math.max(d1.scale(), d2.scale());
            int range = Math.max(d1.precision() - d1.scale(), d2.precision() - d2.scale());
            if (range + scale > 38) {
                // DecimalType can't support precision > 38
                return Optional.of(HIVE_DOUBLE);
            }
            else {
                return Optional.of(new DecimalTypeInfo(range + scale, scale));
            }
        }

        return Optional.empty();
    }

    // ref: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala#L79
    private static Optional<TypeInfo> findTypeForComplex(TypeInfo t1, TypeInfo t2)
    {
        if (isArrayType(t1) && isArrayType(t2)) {
            ListTypeInfo et1 = (ListTypeInfo) t1;
            ListTypeInfo et2 = (ListTypeInfo) t2;
            return findTightestCommonType(et1.getListElementTypeInfo(), et2.getListElementTypeInfo())
                    .map(HiveTypes::arrayType);
        }

        if (isMapType(t1) && isMapType(t2)) {
            MapTypeInfo mt1 = (MapTypeInfo) t1;
            MapTypeInfo mt2 = (MapTypeInfo) t2;
            return findTightestCommonType(mt1.getMapKeyTypeInfo(), mt2.getMapKeyTypeInfo())
                    .flatMap(key -> findTightestCommonType(mt1.getMapValueTypeInfo(), mt2.getMapValueTypeInfo())
                            .map(value -> HiveTypes.mapType(key, value)));
        }

        if (isStructType(t1) && isStructType(t2)) {
            StructTypeInfo st1 = (StructTypeInfo) t1;
            StructTypeInfo st2 = (StructTypeInfo) t2;
            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<TypeInfo> fieldsBuilder = ImmutableList.builder();

            List<String> st1FieldNames = st1.getAllStructFieldNames();
            List<TypeInfo> st1TypeInfos = st1.getAllStructFieldTypeInfos();
            Set<String> st1NameSet = ImmutableSet.copyOf(st1FieldNames);

            List<String> st2FieldNames = st2.getAllStructFieldNames();
            List<TypeInfo> st2TypeInfos = st2.getAllStructFieldTypeInfos();
            Set<String> st2NameSet = ImmutableSet.copyOf(st2FieldNames);

            if (st1.getAllStructFieldNames().size() == st1.getAllStructFieldTypeInfos().size() && st2.getAllStructFieldNames().size() == st2.getAllStructFieldTypeInfos().size()) {
                Map<String, TypeInfo> st1NameToInfo = buildNameToInfoMap(st1FieldNames, st1TypeInfos);
                Map<String, TypeInfo> st2NameToInfo = buildNameToInfoMap(st2FieldNames, st2TypeInfos);

                SetView<String> sameNames = Sets.intersection(st1NameSet, st2NameSet);

                for (String sameName : sameNames) {
                    TypeInfo f1Type = st1NameToInfo.get(sameName);
                    TypeInfo f2Type = st2NameToInfo.get(sameName);

                    Optional<TypeInfo> tightestCommonType = findTightestCommonType(f1Type, f2Type);
                    if (tightestCommonType.isEmpty()) {
                        break;
                    }
                    namesBuilder.add(sameName);
                    fieldsBuilder.add(tightestCommonType.get());
                }

                SetView<String> st1OnlyNames = Sets.difference(st1NameSet, st2NameSet);
                for (String st1OnlyName : st1OnlyNames) {
                    namesBuilder.add(st1OnlyName);
                    fieldsBuilder.add(st1NameToInfo.getOrDefault(st1OnlyName, NullType.NULL_TYPE));
                }

                SetView<String> st2OnlyNames = Sets.difference(st2NameSet, st1NameSet);
                for (String st2OnlyName : st2OnlyNames) {
                    namesBuilder.add(st2OnlyName);
                    fieldsBuilder.add(st2NameToInfo.getOrDefault(st2OnlyName, NullType.NULL_TYPE));
                }

                return Optional.of(structType(namesBuilder.build(), fieldsBuilder.build()));
            }
        }

        return Optional.empty();
    }

    public static boolean isStringType(TypeInfo t)
    {
        return t.equals(STRING_TYPE) || t.equals(STRING_TYPE_ALT);
    }

    public static boolean isDecimalType(TypeInfo t)
    {
        return t instanceof DecimalTypeInfo;
    }

    public static boolean isSmallIntType(TypeInfo t)
    {
        return t.equals(HIVE_SHORT);
    }

    public static boolean isIntType(TypeInfo t)
    {
        return t.equals(HIVE_INT);
    }

    public static boolean isLongType(TypeInfo t)
    {
        return t.equals(HIVE_LONG);
    }

    public static boolean isIntegralType(TypeInfo t)
    {
        return isIntType(t) || isLongType(t);
    }

    public static boolean isDoubleType(TypeInfo t)
    {
        return t.equals(HIVE_DOUBLE);
    }

    public static boolean isNumericType(TypeInfo t)
    {
        return isIntegralType(t) || isDoubleType(t) || isDecimalType(t);
    }

    public static boolean isTimestamp(TypeInfo t)
    {
        return t.equals(HIVE_TIMESTAMP);
    }

    public static boolean isDate(TypeInfo t)
    {
        return t.equals(HIVE_DATE);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L80
    private static boolean isWiderThan(DecimalTypeInfo t1, TypeInfo t2)
    {
        if (isDecimalType(t2)) {
            DecimalTypeInfo dt = (DecimalTypeInfo) t2;
            return (t1.precision() - t1.scale()) >= (dt.precision() - dt.scale()) && t1.scale() >= dt.scale();
        }
        if (isIntegralType(t2)) {
            return isWiderThan(t1, forType(t2));
        }
        return false;
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L137
    private static DecimalTypeInfo forType(TypeInfo type)
    {
        if (isSmallIntType(type)) {
            return ShortDecimal;
        }
        if (isIntType(type)) {
            return IntDecimal;
        }
        if (isLongType(type)) {
            return LongDecimal;
        }
        if (isDoubleType(type)) {
            return DoubleDecimal;
        }
        throw new TrinoException(SchemaDiscoveryErrorCode.UNEXPECTED_DATA_TYPE, "Unexpected decimal type: " + type);
    }

    private TypeCoercion()
    {
    }
}
