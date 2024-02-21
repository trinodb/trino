/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.starburst.schema.discovery.infer.InferType;
import io.starburst.schema.discovery.infer.NullType;
import io.starburst.schema.discovery.infer.TypeCoercion;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.infer.NullType.isNullType;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BOOLEAN;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;

class JsonInferSchema
{
    private final InferType inferType;
    private final JsonController jsonController;

    JsonInferSchema(OptionsMap options, JsonStreamSampler sampler)
    {
        jsonController = new JsonController(sampler);
        inferType = new InferType(new GeneralOptions(options), jsonController.columnNames());
    }

    List<Column> infer()
    {
        Map<String, TypeInfo> startType = buildStartTypeMap(jsonController.columnNames());
        Map<String, TypeInfo> rootTypes = jsonController.stream().reduce(startType, this::inferRowType, this::mergeJsonRowTypes);
        return toStructFields(rootTypes);
    }

    private Map<String, TypeInfo> inferRowType(Map<String, TypeInfo> rowSoFar, JsonNode node)
    {
        return inferRowType(rowSoFar, jsonToChildrenMap(node), this::inferField);
    }

    private TypeInfo inferField(TypeInfo typeSoFar, JsonNode node)
    {
        if (node.isNull() || node.isMissingNode()) {
            return typeSoFar;
        }
        if (node.isBoolean()) {
            return TypeCoercion.compatibleType(typeSoFar, HIVE_BOOLEAN).orElse(STRING_TYPE);
        }
        else if (node.isInt()) {
            return TypeCoercion.compatibleType(typeSoFar, HIVE_INT).orElse(STRING_TYPE);
        }
        else if (node.isLong()) {
            return TypeCoercion.compatibleType(typeSoFar, HIVE_LONG).orElse(STRING_TYPE);
        }
        else if (node.isDouble()) {
            return TypeCoercion.compatibleType(typeSoFar, HIVE_DOUBLE).orElse(STRING_TYPE);
        }
        else if (node.isBigDecimal()) {
            BigDecimal bigDecimal = node.decimalValue();
            if (BigDecimal.valueOf(bigDecimal.doubleValue()).equals(bigDecimal)) {
                return TypeCoercion.compatibleType(typeSoFar, HIVE_DOUBLE).orElse(STRING_TYPE);
            }
            DecimalTypeInfo type = new DecimalTypeInfo(bigDecimal.precision(), bigDecimal.scale());
            return TypeCoercion.compatibleType(typeSoFar, type).orElse(STRING_TYPE);
        }
        else if (node.isArray()) {
            TypeInfo type = buildArrayType(node);
            return TypeCoercion.compatibleType(typeSoFar, type).orElse(STRING_TYPE);
        }
        else if (node.isObject()) {
            TypeInfo type = buildRowType(node);
            return TypeCoercion.compatibleType(typeSoFar, type).orElse(STRING_TYPE);
        }
        return inferType.jsonSpecificInferType(typeSoFar, node);
    }

    private TypeInfo buildArrayType(JsonNode node)
    {
        if (node.isEmpty()) {
            return NullType.NULL_TYPE;
        }
        TypeInfo type = NullType.NULL_TYPE;
        for (JsonNode t : node) {
            type = inferField(type, t);
        }
        return HiveTypes.arrayType(NullType.fixType(type));
    }

    private TypeInfo buildRowType(JsonNode node)
    {
        if (node.isEmpty()) {
            return NullType.NULL_TYPE;
        }
        ImmutableList.Builder<String> namesBuilder = ImmutableList.<String>builder();
        ImmutableList.Builder<TypeInfo> fieldsBuilder = ImmutableList.<TypeInfo>builder();
        Iterator<Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext()) {
            Entry<String, JsonNode> next = iterator.next();
            TypeInfo type = inferField(NullType.NULL_TYPE, next.getValue());
            namesBuilder.add(next.getKey());
            fieldsBuilder.add(type);
        }
        return HiveTypes.structType(namesBuilder.build(), fieldsBuilder.build());
    }

    private Map<String, TypeInfo> mergeJsonRowTypes(Map<String, TypeInfo> first, Map<String, TypeInfo> second)
    {
        ImmutableMap.Builder<String, TypeInfo> builder = ImmutableMap.builder();
        Sets.intersection(first.keySet(), second.keySet()).forEach(commonField -> {
            builder.put(commonField, TypeCoercion.findTightestCommonType(first.get(commonField), second.get(commonField)).orElse(STRING_TYPE));
        });

        Sets.difference(first.keySet(), second.keySet()).forEach(firstNotSecond ->
                builder.put(firstNotSecond, first.get(firstNotSecond)));

        Sets.difference(second.keySet(), first.keySet()).forEach(secondNotFirst ->
                builder.put(secondNotFirst, second.get(secondNotFirst)));

        return builder.buildOrThrow();
    }

    public Map<String, TypeInfo> inferRowType(Map<String, TypeInfo> rowSoFar, Map<String, JsonNode> row, BiFunction<TypeInfo, JsonNode, TypeInfo> inferProc)
    {
        Sets.union(rowSoFar.keySet(), row.keySet()).forEach(jsonField ->
                rowSoFar.put(jsonField, inferProc.apply(rowSoFar.get(jsonField), row.getOrDefault(jsonField, MissingNode.getInstance()))));

        return rowSoFar;
    }

    private Map<String, JsonNode> jsonToChildrenMap(JsonNode node)
    {
        Iterator<String> namesIterator = node.fieldNames();
        Iterator<JsonNode> elementsIterator = node.elements();
        Map<String, JsonNode> children = new LinkedHashMap<>();
        while (namesIterator.hasNext() && elementsIterator.hasNext()) {
            children.put(namesIterator.next(), elementsIterator.next());
        }
        return children;
    }

    public List<Column> toStructFields(Map<String, TypeInfo> rootTypes)
    {
        return rootTypes.entrySet().stream()
                .map(typeInfoEntry -> toColumn(typeInfoEntry.getKey(), typeInfoEntry.getValue()))
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    private Optional<Column> toColumn(String name, TypeInfo typeInfo)
    {
        if (isNullType(typeInfo)) {
            return Optional.empty();
        }
        return HiveTypes.toColumn(name, typeInfo);
    }

    private Map<String, TypeInfo> buildStartTypeMap(List<String> columnNames)
    {
        Map<String, TypeInfo> startType = new LinkedHashMap<>();
        for (String columnName : columnNames) {
            startType.put(columnName, NullType.NULL_TYPE);
        }
        return startType;
    }
}
