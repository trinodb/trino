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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;

import java.math.BigDecimal;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BOOLEAN;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DECIMAL;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_STRING;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;
import static io.starburst.schema.discovery.internal.HiveTypes.accept;

public class InferType
{
    private static final Set<Character> NUMBER_EDGE_CASES = ImmutableSet.of('s', 'S', 'l', 'L', 'f', 'F', 'd', 'D');
    private final GeneralOptions options;
    private final List<String> columnNames;

    public InferType(GeneralOptions options, List<String> columnNames)
    {
        this.options = options;
        this.columnNames = ImmutableList.copyOf(columnNames);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L57
    public List<TypeInfo> buildStartType()
    {
        ArrayList<TypeInfo> startType = new ArrayList<TypeInfo>();
        for (int i = 0; i < columnNames.size(); ++i) {
            startType.add(NullType.NULL_TYPE);
        }
        return startType;
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L82
    public <T> List<TypeInfo> inferRowType(List<TypeInfo> rowSoFar, List<T> row, BiFunction<TypeInfo, T, TypeInfo> inferProc)
    {
        int i = 0;
        while (i < Math.min(rowSoFar.size(), row.size())) {  // May have columns on right missing.
            rowSoFar.set(i, inferProc.apply(rowSoFar.get(i), row.get(i)));
            i += 1;
        }
        return rowSoFar;
    }

    public TypeInfo inferType(TypeInfo typeSoFar, String field)
    {
        if (field == null) {
            return typeSoFar;
        }
        if (field.isEmpty()) {
            return STRING_TYPE;
        }

        TypeInfo typeElemInfer;
        if (typeSoFar.equals(NullType.NULL_TYPE)) {
            typeElemInfer = tryParseInteger(field);
        }
        else if (typeSoFar.equals(HIVE_INT)) {
            typeElemInfer = tryParseInteger(field);
        }
        else if (typeSoFar.equals(HIVE_LONG)) {
            typeElemInfer = tryParseLong(field);
        }
        else if (typeSoFar.equals(HIVE_DOUBLE)) {
            typeElemInfer = tryParseDouble(field);
        }
        else {
            typeElemInfer = tryParseCommonStringImplicitTypes(typeSoFar, field);
        }
        return TypeCoercion.compatibleType(typeSoFar, typeElemInfer).orElse(STRING_TYPE);
    }

    public TypeInfo jsonSpecificInferType(TypeInfo typeSoFar, JsonNode node)
    {
        String value = node.asText();
        if (value == null) {
            return typeSoFar;
        }
        if (value.isEmpty()) {
            return STRING_TYPE;
        }

        TypeInfo typeElemInfer;
        // textual numbers are not allowed in trino json deserializer
        if (typeSoFar.equals(NullType.NULL_TYPE)) {
            typeElemInfer = tryParseDecimal(value);
            if (!options.inferJsonStringToDecimal() && (accept(HIVE_DECIMAL, typeElemInfer) || typeElemInfer.equals(HIVE_DOUBLE))) {
                typeElemInfer = HIVE_STRING;
            }
            if (node.isTextual() && options.inferJsonStringToDecimal() && typeElemInfer.equals(HIVE_DOUBLE) && !isInfOrNan(value)) {
                BigDecimal bigDecimal = new BigDecimal(value);
                typeElemInfer = new DecimalTypeInfo(bigDecimal.precision(), bigDecimal.scale());
            }
        }
        else if (node.isNumber() && typeSoFar.equals(HIVE_INT)) {
            typeElemInfer = tryParseInteger(value);
        }
        else if (node.isNumber() && typeSoFar.equals(HIVE_LONG)) {
            typeElemInfer = tryParseLong(value);
        }
        else if (node.isNumber() && typeSoFar.equals(HIVE_DOUBLE)) {
            typeElemInfer = tryParseDouble(value);
        }
        else {
            typeElemInfer = tryParseCommonStringImplicitTypes(typeSoFar, value);
        }
        return TypeCoercion.compatibleType(typeSoFar, typeElemInfer).orElse(STRING_TYPE);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L91
    public List<TypeInfo> mergeRowTypes(List<TypeInfo> first, List<TypeInfo> second)
    {
        // copied from Scala's zipAll()
        ImmutableList.Builder<TypeInfo> builder = ImmutableList.<TypeInfo>builder();
        Iterator<TypeInfo> these = first.iterator();
        Iterator<TypeInfo> those = second.iterator();
        while (these.hasNext() && those.hasNext()) {
            builder.add(TypeCoercion.findTightestCommonType(these.next(), those.next()).orElse(STRING_TYPE));
        }
        while (these.hasNext()) {
            builder.add(these.next());
        }
        while (those.hasNext()) {
            builder.add(those.next());
        }
        return builder.build();
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L70
    public List<Column> toStructFields(List<TypeInfo> rootTypes)
    {
        return Streams.zip(columnNames.stream(), rootTypes.stream(), HiveTypes::toColumn)
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    private TypeInfo tryParseCommonStringImplicitTypes(TypeInfo typeSoFar, String value)
    {
        if (typeSoFar.equals(HIVE_DATE)) {
            return tryParseDate(value);
        }
        if (typeSoFar.equals(HIVE_DOUBLE)) {
            return tryParseDate(value);
        }
        else if (typeSoFar.equals(HIVE_TIMESTAMP)) {
            return tryParseTimestamp(value);
        }
        else if (typeSoFar.equals(HIVE_BOOLEAN)) {
            return tryParseBoolean(value);
        }
        else if (typeSoFar.equals(STRING_TYPE)) {
            return STRING_TYPE;
        }
        else if (typeSoFar instanceof DecimalTypeInfo) {
            return tryParseDecimal(value);
        }
        else {
            // we did our best, return string
            return STRING_TYPE;
        }
    }

    // ref: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L122
    private boolean isInfOrNan(String field)
    {
        return field.equals(options.nanValue()) || field.equals(options.negativeInf()) || field.equals(options.positiveInf());
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L141
    private TypeInfo tryParseDecimal(String field)
    {
        if (isNumberWithEdgeCaseEnding(field)) {
            return STRING_TYPE;
        }
        return DecimalParser.parse(options.decimalFormat(), field).map(bigDecimal -> {
            // Because many other formats do not support decimal, it reduces the cases for
            // decimals by disallowing values having scale (e.g. `1.1`).
            if (bigDecimal.scale() <= 0) {
                // `DecimalType` conversion can fail when
                //   1. The precision is bigger than 38.
                //   2. scale is bigger than precision.
                return new DecimalTypeInfo(bigDecimal.precision(), bigDecimal.scale());
            }
            else {
                return tryParseDouble(field);
            }
        }).orElseGet(() -> tryParseDouble(field));
    }

    private boolean isNumberWithEdgeCaseEnding(String field)
    {
        return !field.isEmpty() && !isInfOrNan(field) && NUMBER_EDGE_CASES.contains(field.charAt(field.length() - 1));
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L176
    private TypeInfo tryParseBoolean(String field)
    {
        if (field.equalsIgnoreCase("true") || field.equalsIgnoreCase("false")) {
            return HIVE_BOOLEAN;
        }
        return STRING_TYPE;
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L159
    private TypeInfo tryParseDouble(String field)
    {
        if (isNumberWithEdgeCaseEnding(field)) {
            return STRING_TYPE;
        }
        if (isInfOrNan(field)) {
            return HIVE_DOUBLE;
        }
        try {
            Double.parseDouble(field);
            return HIVE_DOUBLE;
        }
        catch (NumberFormatException e) {
            return tryParseDate(field);
        }
    }

    // added by JZ
    private TypeInfo tryParseDate(String field)
    {
        try {
            options.dateFormatter().parse(field);
            return HIVE_DATE;
        }
        catch (DateTimeParseException ignore) {
            // ignore
        }
        return tryParseTimestamp(field);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L167
    private TypeInfo tryParseTimestamp(String field)
    {
        try {
            options.dateTimeFormatter().parse(field);
            return HIVE_TIMESTAMP;
        }
        catch (DateTimeParseException ignore) {
            // ignore
        }
        return tryParseBoolean(field);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L125
    private TypeInfo tryParseInteger(String field)
    {
        if (isNumberWithEdgeCaseEnding(field)) {
            return STRING_TYPE;
        }
        try {
            Integer.parseInt(field);
            return HIVE_INT;
        }
        catch (NumberFormatException e) {
            return tryParseLong(field);
        }
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L133
    private TypeInfo tryParseLong(String field)
    {
        if (isNumberWithEdgeCaseEnding(field)) {
            return STRING_TYPE;
        }
        try {
            Long.parseLong(field);
            return HIVE_LONG;
        }
        catch (NumberFormatException e) {
            return tryParseDecimal(field);
        }
    }
}
