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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

public final class ElasticsearchQueryBuilder
{
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    private ElasticsearchQueryBuilder() {}

    public static JsonNode buildSearchQuery(TupleDomain<ElasticsearchColumnHandle> constraint, Optional<String> query, Map<String, String> regexes, Map<String, String> prefixes, Map<String, String> matchPhrasePrefixes)
    {
        ArrayNode filterClauses = JSON.arrayNode();
        ArrayNode mustNotClauses = JSON.arrayNode();
        ArrayNode mustClauses = JSON.arrayNode();

        if (constraint.getDomains().isPresent()) {
            for (Entry<ElasticsearchColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
                ElasticsearchColumnHandle column = entry.getKey();
                Domain domain = entry.getValue();

                checkArgument(!domain.isNone(), "Unexpected NONE domain for %s", column.name());
                if (!domain.isAll()) {
                    // An analyzed text column that is not exact-match pushable is queried with a full-text match_phrase
                    boolean fullText = !column.supportsPredicates() && column.type() instanceof VarcharType;
                    addPredicateToClauses(filterClauses, mustNotClauses, column.predicateName(), domain, column.type(), fullText);
                }
            }
        }

        regexes.forEach((name, value) -> filterClauses.add(
                JSON.objectNode().set(
                        "bool",
                        boolQuery().set("must", JSON.arrayNode().add(regexpQuery(name, value))))));

        prefixes.forEach((name, value) -> filterClauses.add(prefixQuery(name, value)));

        // On an analyzed text field a match_phrase_prefix keeps the multi-token prefix as a phrase (unlike a per-token prefix query)
        matchPhrasePrefixes.forEach((name, value) -> filterClauses.add(matchPhrasePrefixQuery(name, value)));

        query.ifPresent(q -> mustClauses.add(queryStringQuery(q)));

        if (filterClauses.isEmpty() && mustNotClauses.isEmpty() && mustClauses.isEmpty()) {
            return matchAllQuery();
        }

        ObjectNode boolNode = boolQuery();
        if (!filterClauses.isEmpty()) {
            boolNode.set("filter", filterClauses);
        }
        if (!mustNotClauses.isEmpty()) {
            boolNode.set("must_not", mustNotClauses);
        }
        if (!mustClauses.isEmpty()) {
            boolNode.set("must", mustClauses);
        }
        return JSON.objectNode().set("bool", boolNode);
    }

    public static List<JsonNode> buildSort(List<ElasticsearchColumnSort> sortOrder, boolean hasQuery)
    {
        if (!sortOrder.isEmpty()) {
            ImmutableList.Builder<JsonNode> clauses = ImmutableList.builder();
            for (ElasticsearchColumnSort item : sortOrder) {
                ObjectNode order = JSON.objectNode();
                order.put("order", item.ascending() ? "asc" : "desc");
                order.put("missing", item.nullsFirst() ? "_first" : "_last");
                clauses.add(JSON.objectNode().set(item.field(), order));
            }
            return clauses.build();
        }
        if (hasQuery) {
            // With a custom Elasticsearch query, rely on relevance scoring instead of an explicit sort
            return ImmutableList.of();
        }
        // Sorting by _doc (index order) is the cheapest option for a plain scroll
        return ImmutableList.of(JSON.textNode("_doc"));
    }

    public static ObjectNode buildAggregationQuery(
            JsonNode query,
            List<ElasticsearchColumnHandle> groupingColumns,
            List<ElasticsearchAggregate> aggregates,
            int compositeSize,
            Optional<JsonNode> afterKey)
    {
        ObjectNode body = JSON.objectNode();
        body.put("size", 0);
        body.put("track_total_hits", true);
        body.set("query", query);

        // Metric sub-aggregations. count(*) reads the bucket doc_count / total hits, so it has no sub-aggregation.
        ObjectNode metrics = JSON.objectNode();
        for (ElasticsearchAggregate aggregate : aggregates) {
            Optional<String> metric = metricAggregation(aggregate.function());
            if (metric.isPresent()) {
                metrics.set(aggregate.outputName(), JSON.objectNode().set(metric.get(), JSON.objectNode().put("field", aggregate.field().orElseThrow())));
                if (aggregate.function() == ElasticsearchAggregate.Function.SUM) {
                    // Elasticsearch sum() is 0 over an all-null group, but SQL sum() must be NULL there; value_count detects it
                    metrics.set(aggregate.outputName() + "_count", JSON.objectNode().set("value_count", JSON.objectNode().put("field", aggregate.field().orElseThrow())));
                }
            }
        }

        if (groupingColumns.isEmpty()) {
            // Global aggregation: metrics live at the top level and produce a single result row
            if (!metrics.isEmpty()) {
                body.set("aggs", metrics);
            }
            return body;
        }

        // Grouped aggregation: a composite aggregation paginates deterministically over the grouping keys
        ArrayNode sources = JSON.arrayNode();
        for (int i = 0; i < groupingColumns.size(); i++) {
            // missing_bucket:true keeps documents whose grouping field is null/missing, so the SQL NULL group is not dropped
            ObjectNode termsSource = JSON.objectNode().put("field", groupingColumns.get(i).predicateName()).put("missing_bucket", true);
            ObjectNode terms = JSON.objectNode().set("terms", termsSource);
            sources.add(JSON.objectNode().set("g" + i, terms));
        }
        ObjectNode composite = JSON.objectNode();
        composite.put("size", compositeSize);
        composite.set("sources", sources);
        afterKey.ifPresent(key -> composite.set("after", key));

        ObjectNode groups = JSON.objectNode();
        groups.set("composite", composite);
        if (!metrics.isEmpty()) {
            groups.set("aggs", metrics);
        }
        body.set("aggs", JSON.objectNode().set("groups", groups));
        return body;
    }

    private static Optional<String> metricAggregation(ElasticsearchAggregate.Function function)
    {
        return switch (function) {
            case COUNT_ALL -> Optional.empty();
            case COUNT -> Optional.of("value_count");
            case SUM -> Optional.of("sum");
            case MIN -> Optional.of("min");
            case MAX -> Optional.of("max");
            case AVG -> Optional.of("avg");
            case COUNT_DISTINCT -> Optional.of("cardinality");
        };
    }

    private static void addPredicateToClauses(ArrayNode filterClauses, ArrayNode mustNotClauses, String columnName, Domain domain, Type type, boolean fullText)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            mustNotClauses.add(existsQuery(columnName));
            return;
        }

        if (domain.getValues().isAll()) {
            filterClauses.add(existsQuery(columnName));
            return;
        }

        List<JsonNode> shouldClauses = fullText ? getFullTextClauses(columnName, domain) : getShouldClauses(columnName, domain, type);
        if (shouldClauses.size() == 1) {
            filterClauses.add(getOnlyElement(shouldClauses));
            return;
        }
        if (shouldClauses.size() > 1) {
            ObjectNode boolNode = boolQuery();
            ArrayNode shouldArray = JSON.arrayNode();
            shouldClauses.forEach(shouldArray::add);
            boolNode.set("should", shouldArray);
            filterClauses.add(JSON.objectNode().set("bool", boolNode));
            return;
        }
    }

    private static List<JsonNode> getShouldClauses(String columnName, Domain domain, Type type)
    {
        ImmutableList.Builder<JsonNode> shouldClauses = ImmutableList.builder();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll(), "Invalid range for column: %s", columnName);
            if (range.isSingleValue()) {
                shouldClauses.add(termQuery(columnName, getValue(type, range.getSingleValue())));
            }
            else {
                ObjectNode rangeNode = JSON.objectNode();
                if (!range.isLowUnbounded()) {
                    Object lowBound = getValue(type, range.getLowBoundedValue());
                    if (range.isLowInclusive()) {
                        rangeNode.set("gte", toJsonValue(lowBound));
                    }
                    else {
                        rangeNode.set("gt", toJsonValue(lowBound));
                    }
                }
                if (!range.isHighUnbounded()) {
                    Object highBound = getValue(type, range.getHighBoundedValue());
                    if (range.isHighInclusive()) {
                        rangeNode.set("lte", toJsonValue(highBound));
                    }
                    else {
                        rangeNode.set("lt", toJsonValue(highBound));
                    }
                }
                shouldClauses.add(JSON.objectNode().set(
                        "range",
                        JSON.objectNode().set(columnName, rangeNode)));
            }
        }
        if (domain.isNullAllowed()) {
            ObjectNode mustNotBool = boolQuery();
            mustNotBool.set("must_not", JSON.arrayNode().add(existsQuery(columnName)));
            shouldClauses.add(JSON.objectNode().set("bool", mustNotBool));
        }
        return shouldClauses.build();
    }

    private static List<JsonNode> getFullTextClauses(String columnName, Domain domain)
    {
        ImmutableList.Builder<JsonNode> shouldClauses = ImmutableList.builder();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            // Full text can only express discrete values; a non-single range (for example from a compacted dynamic
            // filter) is left unconstrained here so results stay a superset that the engine or join re-checks
            if (range.isSingleValue()) {
                shouldClauses.add(matchPhraseQuery(columnName, ((Slice) range.getSingleValue()).toStringUtf8()));
            }
        }
        if (domain.isNullAllowed()) {
            ObjectNode mustNotBool = boolQuery();
            mustNotBool.set("must_not", JSON.arrayNode().add(existsQuery(columnName)));
            shouldClauses.add(JSON.objectNode().set("bool", mustNotBool));
        }
        return shouldClauses.build();
    }

    private static ObjectNode matchPhraseQuery(String field, String value)
    {
        return JSON.objectNode().set("match_phrase", JSON.objectNode().put(field, value));
    }

    private static Object getValue(Type type, Object value)
    {
        if (type.equals(BOOLEAN) ||
                type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(DOUBLE)) {
            return value;
        }
        if (type.equals(REAL)) {
            return Float.intBitsToFloat(toIntExact(((Long) value)));
        }
        if (type.equals(VARCHAR)) {
            return ((Slice) value).toStringUtf8();
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return Instant.ofEpochMilli(floorDiv((Long) value, MICROSECONDS_PER_MILLISECOND))
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime()
                    .format(ISO_DATE_TIME);
        }
        throw new IllegalArgumentException("Unhandled type: " + type);
    }

    private static ObjectNode boolQuery()
    {
        return JSON.objectNode();
    }

    private static ObjectNode matchAllQuery()
    {
        return JSON.objectNode().set("match_all", JSON.objectNode());
    }

    private static ObjectNode existsQuery(String field)
    {
        return JSON.objectNode().set(
                "exists",
                JSON.objectNode().put("field", field));
    }

    private static ObjectNode termQuery(String field, Object value)
    {
        return JSON.objectNode().set(
                "term",
                JSON.objectNode().set(field, toJsonValue(value)));
    }

    private static ObjectNode regexpQuery(String field, String value)
    {
        return JSON.objectNode().set(
                "regexp",
                JSON.objectNode().put(field, value));
    }

    private static ObjectNode prefixQuery(String field, String value)
    {
        return JSON.objectNode().set(
                "prefix",
                JSON.objectNode().put(field, value));
    }

    private static ObjectNode matchPhrasePrefixQuery(String field, String value)
    {
        return JSON.objectNode().set(
                "match_phrase_prefix",
                JSON.objectNode().put(field, value));
    }

    private static ObjectNode queryStringQuery(String query)
    {
        return JSON.objectNode().set(
                "query_string",
                JSON.objectNode().put("query", query));
    }

    private static JsonNode toJsonValue(Object object)
    {
        if (object instanceof Boolean value) {
            return JSON.booleanNode(value);
        }
        if (object instanceof Long value) {
            return JSON.numberNode(value);
        }
        if (object instanceof Integer value) {
            return JSON.numberNode(value);
        }
        if (object instanceof Double value) {
            return JSON.numberNode(value);
        }
        if (object instanceof Float value) {
            return JSON.numberNode(value);
        }
        if (object instanceof String value) {
            return JSON.textNode(value);
        }
        throw new IllegalArgumentException("Unsupported value type: " + object.getClass().getSimpleName());
    }
}
