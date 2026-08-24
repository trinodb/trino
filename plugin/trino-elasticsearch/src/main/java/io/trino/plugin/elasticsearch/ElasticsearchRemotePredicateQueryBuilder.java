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
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;

import static java.util.Objects.requireNonNull;

final class ElasticsearchRemotePredicateQueryBuilder
{
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    private ElasticsearchRemotePredicateQueryBuilder() {}

    public static JsonNode build(ElasticsearchRemotePredicate predicate)
    {
        requireNonNull(predicate, "predicate is null");

        return switch (predicate) {
            case ElasticsearchRemotePredicate.And and -> booleanQuery("filter", and.predicates(), false);
            case ElasticsearchRemotePredicate.Or or -> booleanQuery("should", or.predicates(), true);
            case ElasticsearchRemotePredicate.Not not -> JSON.objectNode().set(
                    "bool",
                    JSON.objectNode().set("must_not", JSON.arrayNode().add(build(not.predicate()))));
            case ElasticsearchRemotePredicate.Enforced enforced -> build(enforced.predicate());
            case ElasticsearchRemotePredicate.Term term -> JSON.objectNode().set(
                    "term",
                    JSON.objectNode().set(term.field(), toJsonValue(term.value())));
            case ElasticsearchRemotePredicate.Terms terms -> {
                ArrayNode values = JSON.arrayNode();
                terms.values().forEach(value -> values.add(toJsonValue(value)));
                yield JSON.objectNode().set("terms", JSON.objectNode().set(terms.field(), values));
            }
            case ElasticsearchRemotePredicate.Range range -> {
                ObjectNode bounds = JSON.objectNode();
                range.lower().ifPresent(bound -> bounds.set(bound.inclusive() ? "gte" : "gt", toJsonValue(bound.value())));
                range.upper().ifPresent(bound -> bounds.set(bound.inclusive() ? "lte" : "lt", toJsonValue(bound.value())));
                yield JSON.objectNode().set("range", JSON.objectNode().set(range.field(), bounds));
            }
            case ElasticsearchRemotePredicate.Prefix prefix -> JSON.objectNode().set(
                    "prefix",
                    JSON.objectNode().put(prefix.field(), prefix.value()));
            case ElasticsearchRemotePredicate.Regexp regexp -> JSON.objectNode().set(
                    "regexp",
                    JSON.objectNode().put(regexp.field(), regexp.value()));
            case ElasticsearchRemotePredicate.MatchPhrase matchPhrase -> JSON.objectNode().set(
                    "match_phrase",
                    JSON.objectNode().put(matchPhrase.field(), matchPhrase.value()));
            case ElasticsearchRemotePredicate.MatchPhrasePrefix matchPhrasePrefix -> JSON.objectNode().set(
                    "match_phrase_prefix",
                    JSON.objectNode().put(matchPhrasePrefix.field(), matchPhrasePrefix.value()));
            case ElasticsearchRemotePredicate.Exists exists -> JSON.objectNode().set(
                    "exists",
                    JSON.objectNode().put("field", exists.field()));
        };
    }

    private static ObjectNode booleanQuery(String clauseName, Iterable<ElasticsearchRemotePredicate> predicates, boolean minimumShouldMatch)
    {
        ArrayNode clauses = JSON.arrayNode();
        predicates.forEach(predicate -> clauses.add(build(predicate)));

        ObjectNode bool = JSON.objectNode().set(clauseName, clauses);
        if (minimumShouldMatch) {
            bool.put("minimum_should_match", 1);
        }
        return JSON.objectNode().set("bool", bool);
    }

    private static JsonNode toJsonValue(ElasticsearchRemotePredicate.Value value)
    {
        return switch (value.type()) {
            case BOOLEAN -> JSON.booleanNode(Boolean.parseBoolean(value.value()));
            case LONG -> JSON.numberNode(Long.parseLong(value.value()));
            case DOUBLE -> JSON.numberNode(Double.parseDouble(value.value()));
            case STRING -> JSON.textNode(value.value());
        };
    }
}
