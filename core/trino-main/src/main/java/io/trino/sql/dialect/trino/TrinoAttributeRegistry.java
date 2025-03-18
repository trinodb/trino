/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.AttributeMetadata;
import io.trino.sql.dialect.trino.Attributes.NullableValues;
import io.trino.sql.planner.PartitioningHandle;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.Attributes.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.Attributes.BUCKET_TO_PARTITION;
import static io.trino.sql.dialect.trino.Attributes.CARDINALITY;
import static io.trino.sql.dialect.trino.Attributes.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.Attributes.COMPARISON_OPERATOR;
import static io.trino.sql.dialect.trino.Attributes.CONSTANT_RESULT;
import static io.trino.sql.dialect.trino.Attributes.CONSTRAINT;
import static io.trino.sql.dialect.trino.Attributes.DISTINCT;
import static io.trino.sql.dialect.trino.Attributes.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.Attributes.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_TYPE;
import static io.trino.sql.dialect.trino.Attributes.FIELD_INDEX;
import static io.trino.sql.dialect.trino.Attributes.GLOBAL_GROUPING_SETS;
import static io.trino.sql.dialect.trino.Attributes.GROUPING_SETS_COUNT;
import static io.trino.sql.dialect.trino.Attributes.GROUP_ID_INDEX;
import static io.trino.sql.dialect.trino.Attributes.INPUT_REDUCING;
import static io.trino.sql.dialect.trino.Attributes.JOIN_TYPE;
import static io.trino.sql.dialect.trino.Attributes.LIMIT;
import static io.trino.sql.dialect.trino.Attributes.LOGICAL_OPERATOR;
import static io.trino.sql.dialect.trino.Attributes.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.Attributes.NULLABLE_VALUES;
import static io.trino.sql.dialect.trino.Attributes.OUTPUT_NAMES;
import static io.trino.sql.dialect.trino.Attributes.PARTIAL;
import static io.trino.sql.dialect.trino.Attributes.PARTITIONING_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.PARTITION_COUNT;
import static io.trino.sql.dialect.trino.Attributes.PRE_GROUPED_INDEXES;
import static io.trino.sql.dialect.trino.Attributes.PRE_SORTED_INDEXES;
import static io.trino.sql.dialect.trino.Attributes.REPLICATE_NULLS_AND_ANY;
import static io.trino.sql.dialect.trino.Attributes.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.Attributes.RESOLVED_FUNCTION_CODEC;
import static io.trino.sql.dialect.trino.Attributes.SORT_ORDERS;
import static io.trino.sql.dialect.trino.Attributes.SPILLABLE;
import static io.trino.sql.dialect.trino.Attributes.STATISTICS;
import static io.trino.sql.dialect.trino.Attributes.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.dialect.trino.Attributes.STATISTICS_AND_COST_SUMMARY_CODEC;
import static io.trino.sql.dialect.trino.Attributes.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.TOP_N_STEP;
import static io.trino.sql.dialect.trino.Attributes.UPDATE_TARGET;
import static io.trino.sql.dialect.trino.Attributes.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.dialect.trino.TrinoAttributeRegistry.ConstantResult.CONSTANT_RESULT_CODEC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TrinoAttributeRegistry
{
    private static final Map<String, AttributeProperties<?>> STATIC_ATTRIBUTE_PROPERTIES = staticAttributeProperties();

    public static final TrinoAttributeRegistry TESTING_TRINO_ATTRIBUTE_REGISTRY = testingTrinoAttributeRegistry();

    private final Map<String, AttributeProperties<?>> attributeProperties;

    @Inject
    public TrinoAttributeRegistry(
            JsonCodec<TableHandle> tableHandleCodec,
            JsonCodec<List<ColumnHandle>> columnHandleCodec,
            JsonCodec<TupleDomain<ColumnHandle>> tupleDomainCodec,
            JsonCodec<PartitioningHandle> partitioningHandleCodec,
            JsonCodec<NullableValue> nullableValueCodec,
            JsonCodec<NullableValue[]> nullableValueArrayCodec)
    {
        this(buildAttributeProperties(tableHandleCodec, columnHandleCodec, tupleDomainCodec, partitioningHandleCodec, nullableValueCodec, nullableValueArrayCodec));
    }

    private TrinoAttributeRegistry(Map<String, AttributeProperties<?>> attributeProperties)
    {
        this.attributeProperties = requireNonNull(attributeProperties, "attributeProperties is null");
    }

    public AttributeProperties<?> getAttributeProperties(String name)
    {
        AttributeProperties<?> properties = attributeProperties.get(name);
        if (properties == null) {
            throw new TrinoException(IR_ERROR, format("attribute %s not registered", name));
        }

        return properties;
    }

    private static Map<String, AttributeProperties<?>> staticAttributeProperties()
    {
        return ImmutableMap.<String, AttributeProperties<?>>builder()
                .put(AGGREGATION_STEP.name(), new AttributeProperties<>(AGGREGATION_STEP, Attributes.AggregationStep::parse, Attributes.AggregationStep::print))
                .put(BUCKET_TO_PARTITION.name(), new AttributeProperties<>(BUCKET_TO_PARTITION, Attributes.IntegerList::parse, Attributes.IntegerList::print))
                .put(CARDINALITY.name(), new AttributeProperties<>(CARDINALITY, Long::valueOf, Object::toString))
                .put(COMPARISON_OPERATOR.name(), new AttributeProperties<>(COMPARISON_OPERATOR, Attributes.ComparisonOperator::parse, Attributes.ComparisonOperator::print))
                .put(DISTINCT.name(), new AttributeProperties<>(DISTINCT, Boolean::valueOf, Object::toString))
                .put(DISTRIBUTION_TYPE.name(), new AttributeProperties<>(DISTRIBUTION_TYPE, Attributes.DistributionType::parse, Attributes.DistributionType::print))
                .put(DYNAMIC_FILTER_IDS.name(), new AttributeProperties<>(DYNAMIC_FILTER_IDS, Attributes.StringList::parse, Attributes.StringList::print))
                .put(EXCHANGE_SCOPE.name(), new AttributeProperties<>(EXCHANGE_SCOPE, Attributes.ExchangeScope::parse, Attributes.ExchangeScope::print))
                .put(EXCHANGE_TYPE.name(), new AttributeProperties<>(EXCHANGE_TYPE, Attributes.ExchangeType::parse, Attributes.ExchangeType::print))
                .put(FIELD_INDEX.name(), new AttributeProperties<>(FIELD_INDEX, Integer::valueOf, Object::toString))
                .put(GLOBAL_GROUPING_SETS.name(), new AttributeProperties<>(GLOBAL_GROUPING_SETS, Attributes.IntegerList::parse, Attributes.IntegerList::print))
                .put(GROUPING_SETS_COUNT.name(), new AttributeProperties<>(GROUPING_SETS_COUNT, Integer::valueOf, Object::toString))
                .put(GROUP_ID_INDEX.name(), new AttributeProperties<>(GROUP_ID_INDEX, Integer::valueOf, Object::toString))
                .put(INPUT_REDUCING.name(), new AttributeProperties<>(INPUT_REDUCING, Boolean::valueOf, Object::toString))
                .put(JOIN_TYPE.name(), new AttributeProperties<>(JOIN_TYPE, Attributes.JoinType::parse, Attributes.JoinType::print))
                .put(LIMIT.name(), new AttributeProperties<>(LIMIT, Long::valueOf, Object::toString))
                .put(LOGICAL_OPERATOR.name(), new AttributeProperties<>(LOGICAL_OPERATOR, Attributes.LogicalOperator::parse, Attributes.LogicalOperator::print))
                .put(MAY_SKIP_OUTPUT_DUPLICATES.name(), new AttributeProperties<>(MAY_SKIP_OUTPUT_DUPLICATES, Boolean::valueOf, Object::toString))
                .put(OUTPUT_NAMES.name(), new AttributeProperties<>(OUTPUT_NAMES, Attributes.StringList::parse, Attributes.StringList::print))
                .put(PARTIAL.name(), new AttributeProperties<>(PARTIAL, Boolean::valueOf, Object::toString))
                .put(PARTITION_COUNT.name(), new AttributeProperties<>(PARTITION_COUNT, Integer::valueOf, Object::toString))
                .put(PRE_GROUPED_INDEXES.name(), new AttributeProperties<>(PRE_GROUPED_INDEXES, Attributes.IntegerList::parse, Attributes.IntegerList::print))
                .put(PRE_SORTED_INDEXES.name(), new AttributeProperties<>(PRE_SORTED_INDEXES, Attributes.IntegerList::parse, Attributes.IntegerList::print))
                .put(REPLICATE_NULLS_AND_ANY.name(), new AttributeProperties<>(REPLICATE_NULLS_AND_ANY, Boolean::valueOf, Object::toString))
                .put(RESOLVED_FUNCTION.name(), new AttributeProperties<>(RESOLVED_FUNCTION, RESOLVED_FUNCTION_CODEC::fromJson, RESOLVED_FUNCTION_CODEC::toJson))
                .put(SORT_ORDERS.name(), new AttributeProperties<>(SORT_ORDERS, Attributes.SortOrderList::parse, Attributes.SortOrderList::print))
                .put(SPILLABLE.name(), new AttributeProperties<>(SPILLABLE, Boolean::valueOf, Object::toString))
                .put(STATISTICS.name(), new AttributeProperties<>(STATISTICS, Attributes.Statistics::parse, Attributes.Statistics::print))
                .put(STATISTICS_AND_COST_SUMMARY.name(), new AttributeProperties<>(STATISTICS_AND_COST_SUMMARY, STATISTICS_AND_COST_SUMMARY_CODEC::fromJson, STATISTICS_AND_COST_SUMMARY_CODEC::toJson))
                .put(TOP_N_STEP.name(), new AttributeProperties<>(TOP_N_STEP, Attributes.TopNStep::parse, Attributes.TopNStep::print))
                .put(UPDATE_TARGET.name(), new AttributeProperties<>(UPDATE_TARGET, Boolean::valueOf, Object::toString))
                .put(USE_CONNECTOR_NODE_PARTITIONING.name(), new AttributeProperties<>(USE_CONNECTOR_NODE_PARTITIONING, Boolean::valueOf, Object::toString))
                .buildOrThrow();
    }

    private static Map<String, AttributeProperties<?>> buildAttributeProperties(
            JsonCodec<TableHandle> tableHandleCodec,
            JsonCodec<List<ColumnHandle>> columnHandleCodec,
            JsonCodec<TupleDomain<ColumnHandle>> tupleDomainCodec,
            JsonCodec<PartitioningHandle> partitioningHandleCodec,
            JsonCodec<NullableValue> nullableValueCodec,
            JsonCodec<NullableValue[]> nullableValueArrayCodec)
    {
        requireNonNull(tableHandleCodec, "tableHandleCodec is null");
        requireNonNull(columnHandleCodec, "columnHandleCodec is null");
        requireNonNull(tupleDomainCodec, "tupleDomainCodec is null");
        requireNonNull(partitioningHandleCodec, "partitioningHandleCodec is null");
        requireNonNull(nullableValueCodec, "nullableValueCodec is null");
        requireNonNull(nullableValueArrayCodec, "nullableValueArrayCodec is null");

        return ImmutableMap.<String, AttributeProperties<?>>builder()
                .putAll(STATIC_ATTRIBUTE_PROPERTIES)
                .put(COLUMN_HANDLES.name(), new AttributeProperties<>(COLUMN_HANDLES, columnHandleCodec::fromJson, columnHandleCodec::toJson))
                .put(CONSTANT_RESULT.name(), new AttributeProperties<>(CONSTANT_RESULT, nullableValueCodec::fromJson, nullableValueCodec::toJson))
                .put(CONSTRAINT.name(), new AttributeProperties<>(CONSTRAINT, tupleDomainCodec::fromJson, tupleDomainCodec::toJson))
                .put(NULLABLE_VALUES.name(), new AttributeProperties<>(
                        NULLABLE_VALUES,
                        string -> new NullableValues(nullableValueArrayCodec.fromJson(string)),
                        nullableValues -> nullableValueArrayCodec.toJson(nullableValues.nullableValues())))
                .put(PARTITIONING_HANDLE.name(), new AttributeProperties<>(PARTITIONING_HANDLE, partitioningHandleCodec::fromJson, partitioningHandleCodec::toJson))
                .put(TABLE_HANDLE.name(), new AttributeProperties<>(TABLE_HANDLE, tableHandleCodec::fromJson, tableHandleCodec::toJson))
                .buildOrThrow();
    }

    public record AttributeProperties<T>(AttributeMetadata<T> attributeMetadata, Function<String, T> parseMethod, Function<T, String> printMethod)
    {
        public AttributeProperties
        {
            requireNonNull(attributeMetadata, "attributeMetadata is null");
            requireNonNull(parseMethod, "parseMethod is null");
            requireNonNull(printMethod, "printMethod is null");
        }

        public T parse(String string)
        {
            return parseMethod.apply(string);
        }

        @SuppressWarnings("unchecked")
        public String print(Object attribute)
        {
            return printMethod.apply((T) attribute);
        }
    }

    private static TrinoAttributeRegistry testingTrinoAttributeRegistry()
    {
        Map<String, AttributeProperties<?>> attributeProperties = ImmutableMap.<String, AttributeProperties<?>>builder()
                .putAll(STATIC_ATTRIBUTE_PROPERTIES)
                .put(
                        COLUMN_HANDLES.name(),
                        new AttributeProperties<>(
                                COLUMN_HANDLES,
                                _ -> {
                                    throw new UnsupportedOperationException("cannot parse column_handles attribute in TESTING_TRINO_ATTRIBUTE_REGISTRY");
                                },
                                _ -> "[test: column_handles attribute]"))
                .put(
                        CONSTANT_RESULT.name(),
                        new AttributeProperties<>(
                                CONSTANT_RESULT,
                                _ -> {
                                    throw new UnsupportedOperationException("cannot parse constant_result attribute in TESTING_TRINO_ATTRIBUTE_REGISTRY");
                                },
                                nullableValue -> {
                                    ConstantResult constantResult = new ConstantResult(nullableValue.getType(), nullableValue.getValue());
                                    try {
                                        return CONSTANT_RESULT_CODEC.toJson(constantResult);
                                    }
                                    catch (IllegalArgumentException e) {
                                        return "[test: constant_result attribute]";
                                    }
                                }))
                .put(
                        CONSTRAINT.name(),
                        new AttributeProperties<>(
                                CONSTRAINT,
                                _ -> {
                                    throw new UnsupportedOperationException("cannot parse constraint attribute in TESTING_TRINO_ATTRIBUTE_REGISTRY");
                                },
                                _ -> "[test: constraint attribute]"))
                .put(
                        NULLABLE_VALUES.name(),
                        new AttributeProperties<>(
                                NULLABLE_VALUES,
                                _ -> {
                                    throw new UnsupportedOperationException("cannot parse nullable_values attribute in TESTING_TRINO_ATTRIBUTE_REGISTRY");
                                },
                                _ -> "[test: nullable_values attribute]"))
                .put(
                        PARTITIONING_HANDLE.name(),
                        new AttributeProperties<>(
                                PARTITIONING_HANDLE,
                                _ -> {
                                    throw new UnsupportedOperationException("cannot parse partitioning_handle attribute in TESTING_TRINO_ATTRIBUTE_REGISTRY");
                                },
                                _ -> "[test: partitioning_handle attribute]"))
                .put(
                        TABLE_HANDLE.name(),
                        new AttributeProperties<>(
                                TABLE_HANDLE,
                                _ -> {
                                    throw new UnsupportedOperationException("cannot parse table_handle attribute in TESTING_TRINO_ATTRIBUTE_REGISTRY");
                                },
                                _ -> "[test: table_handle attribute]"))
                .buildOrThrow();

        return new TrinoAttributeRegistry(attributeProperties);
    }

    public record ConstantResult(Type type, Object value)
    {
        public static final JsonCodec<ConstantResult> CONSTANT_RESULT_CODEC = new JsonCodecFactory().jsonCodec(ConstantResult.class);

        public ConstantResult
        {
            requireNonNull(type, "type is null");
        }
    }
}
