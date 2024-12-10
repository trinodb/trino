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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Logical;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TopNNode;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static java.util.Objects.requireNonNull;

public class Attributes
{
    public static final AttributeMetadata<AggregationStep> AGGREGATION_STEP = new AttributeMetadata<>("aggregation_step", false);
    public static final AttributeMetadata<List<Integer>> BUCKET_TO_PARTITION = new AttributeMetadata<>("bucket_to_partition", false);
    public static final AttributeMetadata<Long> CARDINALITY = new AttributeMetadata<>("cardinality", true);
    public static final AttributeMetadata<List<ColumnHandle>> COLUMN_HANDLES = new AttributeMetadata<>("column_handles", false);
    public static final AttributeMetadata<ComparisonOperator> COMPARISON_OPERATOR = new AttributeMetadata<>("comparison_operator", false);
    public static final AttributeMetadata<NullableValue> CONSTANT_RESULT = new AttributeMetadata<>("constant_result", true);
    public static final AttributeMetadata<TupleDomain<ColumnHandle>> CONSTRAINT = new AttributeMetadata<>("constraint", true);
    public static final AttributeMetadata<Boolean> DISTINCT = new AttributeMetadata<>("distinct", false);
    public static final AttributeMetadata<DistributionType> DISTRIBUTION_TYPE = new AttributeMetadata<>("distribution_type", false);
    public static final AttributeMetadata<List<String>> DYNAMIC_FILTER_IDS = new AttributeMetadata<>("dynamic_filter_ids", false);
    public static final AttributeMetadata<ExchangeScope> EXCHANGE_SCOPE = new AttributeMetadata<>("exchange_scope", false);
    public static final AttributeMetadata<ExchangeType> EXCHANGE_TYPE = new AttributeMetadata<>("exchange_type", false);
    public static final AttributeMetadata<Integer> FIELD_INDEX = new AttributeMetadata<>("field_index", false);
    public static final AttributeMetadata<String> FIELD_NAME = new AttributeMetadata<>("field_name", false);
    public static final AttributeMetadata<List<Integer>> GLOBAL_GROUPING_SETS = new AttributeMetadata<>("global_grouping_sets", false);
    public static final AttributeMetadata<Integer> GROUPING_SETS_COUNT = new AttributeMetadata<>("grouping_sets_count", false);
    public static final AttributeMetadata<Integer> GROUP_ID_INDEX = new AttributeMetadata<>("group_id_index", false);
    public static final AttributeMetadata<Boolean> INPUT_REDUCING = new AttributeMetadata<>("input_reducing", false);
    public static final AttributeMetadata<JoinType> JOIN_TYPE = new AttributeMetadata<>("join_type", false);
    public static final AttributeMetadata<Long> LIMIT = new AttributeMetadata<>("limit", false);
    public static final AttributeMetadata<LogicalOperator> LOGICAL_OPERATOR = new AttributeMetadata<>("logical_operator", false);
    public static final AttributeMetadata<Boolean> MAY_SKIP_OUTPUT_DUPLICATES = new AttributeMetadata<>("may_skip_output_duplicates", false);
    public static final AttributeMetadata<NullableValues> NULLABLE_VALUES = new AttributeMetadata<>("nullable_values", false);
    public static final AttributeMetadata<List<String>> OUTPUT_NAMES = new AttributeMetadata<>("output_names", false);
    public static final AttributeMetadata<Boolean> PARTIAL = new AttributeMetadata<>("partial", false);
    public static final AttributeMetadata<PartitioningHandle> PARTITIONING_HANDLE = new AttributeMetadata<>("partitioning_handle", false);
    public static final AttributeMetadata<Integer> PARTITION_COUNT = new AttributeMetadata<>("partition_count", false);
    public static final AttributeMetadata<List<Integer>> PRE_GROUPED_INDEXES = new AttributeMetadata<>("pre_grouped_indexes", false);
    public static final AttributeMetadata<List<Integer>> PRE_SORTED_INDEXES = new AttributeMetadata<>("pre_sorted_indexes", false);
    public static final AttributeMetadata<Boolean> REPLICATE_NULLS_AND_ANY = new AttributeMetadata<>("replicate_nulls_and_any", false);
    public static final AttributeMetadata<ResolvedFunction> RESOLVED_FUNCTION = new AttributeMetadata<>("resolved_function", false);
    public static final AttributeMetadata<SortOrderList> SORT_ORDERS = new AttributeMetadata<>("sort_orders", true);
    public static final AttributeMetadata<Boolean> SPILLABLE = new AttributeMetadata<>("spillable", false);
    public static final AttributeMetadata<Statistics> STATISTICS = new AttributeMetadata<>("statistics", true);
    public static final AttributeMetadata<PlanNodeStatsAndCostSummary> STATISTICS_AND_COST_SUMMARY = new AttributeMetadata<>("statistics_and_cost_summary", true);
    public static final AttributeMetadata<TableHandle> TABLE_HANDLE = new AttributeMetadata<>("table_handle", false);
    public static final AttributeMetadata<TopNStep> TOP_N_STEP = new AttributeMetadata<>("top_n_step", false);
    public static final AttributeMetadata<Boolean> UPDATE_TARGET = new AttributeMetadata<>("update_target", false);
    public static final AttributeMetadata<Boolean> USE_CONNECTOR_NODE_PARTITIONING = new AttributeMetadata<>("use_connector_node_partitioning", false);

    // TODO define attributes for deeply nested fields, not just top level or column level

    public static final JsonCodecFactory JSON_CODEC_FACTORY = new JsonCodecFactory();
    public static final JsonCodec<ResolvedFunction> RESOLVED_FUNCTION_CODEC = JSON_CODEC_FACTORY.jsonCodec(ResolvedFunction.class);
    public static final JsonCodec<PlanNodeStatsAndCostSummary> STATISTICS_AND_COST_SUMMARY_CODEC = JSON_CODEC_FACTORY.jsonCodec(PlanNodeStatsAndCostSummary.class);

    private Attributes() {}

    public record AttributeMetadata<T>(String name, boolean external)
    {
        public AttributeMetadata
        {
            requireNonNull(name, "name is null");
            if (name.isEmpty()) {
                throw new TrinoException(IR_ERROR, "attribute name is empty");
            }
        }

        @SuppressWarnings("unchecked")
        public T getAttribute(Map<AttributeKey, Object> map)
        {
            return (T) map.get(new AttributeKey(TRINO, name));
        }

        public void putAttribute(ImmutableMap.Builder<AttributeKey, Object> builder, T attribute)
        {
            builder.put(new AttributeKey(TRINO, name), attribute);
        }

        @SuppressWarnings("unchecked")
        public T putAttribute(Map<AttributeKey, Object> map, T attribute)
        {
            return (T) map.put(new AttributeKey(TRINO, name), attribute);
        }

        public Map<AttributeKey, Object> asMap(T attribute)
        {
            return ImmutableMap.of(new AttributeKey(TRINO, name), attribute);
        }
    }

    public record Statistics(double outputRowCount, PMap<Integer, SymbolStatsEstimate> fieldStatistics)
    {
        private static final JsonCodec<Statistics> STATISTICS_CODEC = JSON_CODEC_FACTORY.jsonCodec(Statistics.class);

        public Statistics
        {
            requireNonNull(fieldStatistics, "fieldStatistics is null");
        }

        public Statistics(double outputRowCount, Map<Integer, SymbolStatsEstimate> fieldStatistics)
        {
            this(outputRowCount, HashTreePMap.from(requireNonNull(fieldStatistics, "fieldStatistics is null")));
        }

        public static Statistics parse(String string)
        {
            return STATISTICS_CODEC.fromJson(string);
        }

        public static String print(Statistics statistics)
        {
            return STATISTICS_CODEC.toJson(statistics);
        }
    }

    public record NullableValues(NullableValue[] nullableValues)
    {
        public NullableValues
        {
            requireNonNull(nullableValues, "nullableValues is null");
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (NullableValues) obj;
            return Arrays.equals(nullableValues, that.nullableValues);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(this.nullableValues);
        }
    }

    public record SortOrderList(List<SortOrder> sortOrders)
    {
        private static final JsonCodec<List<SortOrder>> SORT_ORDERS_CODEC = JSON_CODEC_FACTORY.listJsonCodec(JSON_CODEC_FACTORY.jsonCodec(SortOrder.class));

        public SortOrderList(List<SortOrder> sortOrders)
        {
            requireNonNull(sortOrders, "sortOrders is null");
            if (sortOrders.isEmpty()) {
                throw new TrinoException(IR_ERROR, "sortOrders is empty");
            }
            this.sortOrders = ImmutableList.copyOf(sortOrders);
        }

        public static SortOrderList parse(String string)
        {
            return new SortOrderList(SORT_ORDERS_CODEC.fromJson(string));
        }

        public static String print(SortOrderList sortOrderList)
        {
            return SORT_ORDERS_CODEC.toJson(sortOrderList.sortOrders());
        }
    }

    public record IntegerList()
    {
        private static final JsonCodec<List<Integer>> INTEGER_LIST_CODEC = JSON_CODEC_FACTORY.listJsonCodec(Integer.class);

        public static List<Integer> parse(String string)
        {
            return ImmutableList.copyOf(INTEGER_LIST_CODEC.fromJson(string));
        }

        public static String print(List<Integer> integerList)
        {
            return INTEGER_LIST_CODEC.toJson(integerList);
        }
    }

    public record StringList()
    {
        private static final JsonCodec<List<String>> STRING_LIST_CODEC = JSON_CODEC_FACTORY.listJsonCodec(String.class);

        public static List<String> parse(String string)
        {
            return ImmutableList.copyOf(STRING_LIST_CODEC.fromJson(string));
        }

        public static String print(List<String> stringList)
        {
            return STRING_LIST_CODEC.toJson(stringList);
        }
    }

    public enum JoinType
    {
        INNER,
        LEFT,
        RIGHT,
        FULL;

        public static JoinType of(io.trino.sql.planner.plan.JoinType joinType)
        {
            return switch (joinType) {
                case INNER -> INNER;
                case LEFT -> LEFT;
                case RIGHT -> RIGHT;
                case FULL -> FULL;
            };
        }

        public static JoinType parse(String string)
        {
            return switch (string) {
                case "INNER" -> INNER;
                case "LEFT" -> LEFT;
                case "RIGHT" -> RIGHT;
                case "FULL" -> FULL;
                default -> throw new TrinoException(IR_ERROR, "cannot parse join type: " + string);
            };
        }

        public static String print(JoinType type)
        {
            return type.name();
        }
    }

    public enum LogicalOperator
    {
        AND,
        OR;

        public static LogicalOperator of(Logical.Operator operator)
        {
            return switch (operator) {
                case AND -> AND;
                case OR -> OR;
            };
        }

        public static LogicalOperator parse(String string)
        {
            return switch (string) {
                case "AND" -> AND;
                case "OR" -> OR;
                default -> throw new TrinoException(IR_ERROR, "cannot parse logical operator: " + string);
            };
        }

        public static String print(LogicalOperator operator)
        {
            return operator.name();
        }
    }

    public enum ComparisonOperator
    {
        EQUAL("="),
        NOT_EQUAL("<>"),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">="),
        IDENTICAL("≡"); // not distinct

        private final String value;

        ComparisonOperator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        public static ComparisonOperator of(Comparison.Operator operator)
        {
            return switch (operator) {
                case EQUAL -> EQUAL;
                case NOT_EQUAL -> NOT_EQUAL;
                case LESS_THAN -> LESS_THAN;
                case LESS_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
                case GREATER_THAN -> GREATER_THAN;
                case GREATER_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
                case IDENTICAL -> IDENTICAL;
            };
        }

        public static ComparisonOperator parse(String string)
        {
            return switch (string) {
                case "EQUAL" -> EQUAL;
                case "NOT_EQUAL" -> NOT_EQUAL;
                case "LESS_THAN" -> LESS_THAN;
                case "LESS_THAN_OR_EQUAL" -> LESS_THAN_OR_EQUAL;
                case "GREATER_THAN" -> GREATER_THAN;
                case "GREATER_THAN_OR_EQUAL" -> GREATER_THAN_OR_EQUAL;
                case "IDENTICAL" -> IDENTICAL;
                default -> throw new TrinoException(IR_ERROR, "cannot parse comparison operator: " + string);
            };
        }

        public static String print(ComparisonOperator operator)
        {
            return operator.name();
        }
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED;

        public static DistributionType of(JoinNode.DistributionType distributionType)
        {
            return switch (distributionType) {
                case PARTITIONED -> PARTITIONED;
                case REPLICATED -> REPLICATED;
            };
        }

        public static DistributionType parse(String string)
        {
            return switch (string) {
                case "PARTITIONED" -> PARTITIONED;
                case "REPLICATED" -> REPLICATED;
                default -> throw new TrinoException(IR_ERROR, "cannot parse join distribution type: " + string);
            };
        }

        public static String print(DistributionType type)
        {
            return type.name();
        }
    }

    public enum AggregationStep
    {
        PARTIAL,
        FINAL,
        INTERMEDIATE,
        SINGLE;

        public static AggregationStep of(AggregationNode.Step step)
        {
            return switch (step) {
                case PARTIAL -> PARTIAL;
                case FINAL -> FINAL;
                case INTERMEDIATE -> INTERMEDIATE;
                case SINGLE -> SINGLE;
            };
        }

        public static AggregationStep parse(String string)
        {
            return switch (string) {
                case "PARTIAL" -> PARTIAL;
                case "FINAL" -> FINAL;
                case "INTERMEDIATE" -> INTERMEDIATE;
                case "SINGLE" -> SINGLE;
                default -> throw new TrinoException(IR_ERROR, "cannot parse aggregation step: " + string);
            };
        }

        public static String print(AggregationStep step)
        {
            return step.name();
        }
    }

    public enum TopNStep
    {
        SINGLE,
        PARTIAL,
        FINAL;

        public static TopNStep of(TopNNode.Step step)
        {
            return switch (step) {
                case SINGLE -> SINGLE;
                case PARTIAL -> PARTIAL;
                case FINAL -> FINAL;
            };
        }

        public static TopNStep parse(String string)
        {
            return switch (string) {
                case "SINGLE" -> SINGLE;
                case "PARTIAL" -> PARTIAL;
                case "FINAL" -> FINAL;
                default -> throw new TrinoException(IR_ERROR, "cannot parse topN step: " + string);
            };
        }

        public static String print(TopNStep step)
        {
            return step.name();
        }
    }

    public enum ExchangeType
    {
        GATHER,
        REPARTITION,
        REPLICATE;

        public static ExchangeType of(ExchangeNode.Type type)
        {
            return switch (type) {
                case GATHER -> GATHER;
                case REPARTITION -> REPARTITION;
                case REPLICATE -> REPLICATE;
            };
        }

        public static ExchangeType parse(String string)
        {
            return switch (string) {
                case "GATHER" -> GATHER;
                case "REPARTITION" -> REPARTITION;
                case "REPLICATE" -> REPLICATE;
                default -> throw new TrinoException(IR_ERROR, "cannot parse exchange type: " + string);
            };
        }

        public static String print(ExchangeType type)
        {
            return type.name();
        }
    }

    public enum ExchangeScope
    {
        LOCAL,
        REMOTE;

        public static ExchangeScope of(ExchangeNode.Scope scope)
        {
            return switch (scope) {
                case LOCAL -> LOCAL;
                case REMOTE -> REMOTE;
            };
        }

        public static ExchangeScope parse(String string)
        {
            return switch (string) {
                case "LOCAL" -> LOCAL;
                case "REMOTE" -> REMOTE;
                default -> throw new TrinoException(IR_ERROR, "cannot parse exchange scope: " + string);
            };
        }

        public static String print(ExchangeScope scope)
        {
            return scope.name();
        }
    }
}
