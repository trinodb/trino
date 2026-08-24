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
package io.trino.plugin.elasticsearch.expression;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Connector-owned representation of a predicate that can be rendered as Elasticsearch Query DSL.
 *
 * <p>The IR deliberately models Elasticsearch's index-level predicate primitives rather than SQL expressions. Exact
 * predicates use {@link Enforcement#EXACT} by default. Translation code wraps candidate-only or intentionally
 * approximate predicates in {@link Enforced} so enforcement semantics survive planning, serialization and execution.</p>
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.And.class, name = "and"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Or.class, name = "or"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Not.class, name = "not"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Enforced.class, name = "enforced"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Term.class, name = "term"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Terms.class, name = "terms"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Range.class, name = "range"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Prefix.class, name = "prefix"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Regexp.class, name = "regexp"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.MatchPhrase.class, name = "matchPhrase"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.MatchPhrasePrefix.class, name = "matchPhrasePrefix"),
        @JsonSubTypes.Type(value = ElasticsearchRemotePredicate.Exists.class, name = "exists"),
})
public sealed interface ElasticsearchRemotePredicate
        permits ElasticsearchRemotePredicate.And,
                ElasticsearchRemotePredicate.Enforced,
                ElasticsearchRemotePredicate.Exists,
                ElasticsearchRemotePredicate.MatchPhrase,
                ElasticsearchRemotePredicate.MatchPhrasePrefix,
                ElasticsearchRemotePredicate.Not,
                ElasticsearchRemotePredicate.Or,
                ElasticsearchRemotePredicate.Prefix,
                ElasticsearchRemotePredicate.Range,
                ElasticsearchRemotePredicate.Regexp,
                ElasticsearchRemotePredicate.Term,
                ElasticsearchRemotePredicate.Terms
{
    enum Enforcement
    {
        EXACT,
        PREFILTER,
        APPROXIMATE,
    }

    default Enforcement enforcement()
    {
        return Enforcement.EXACT;
    }

    @JsonProperty("@type")
    default String type()
    {
        return switch (this) {
            case And _ -> "and";
            case Or _ -> "or";
            case Not _ -> "not";
            case Enforced _ -> "enforced";
            case Term _ -> "term";
            case Terms _ -> "terms";
            case Range _ -> "range";
            case Prefix _ -> "prefix";
            case Regexp _ -> "regexp";
            case MatchPhrase _ -> "matchPhrase";
            case MatchPhrasePrefix _ -> "matchPhrasePrefix";
            case Exists _ -> "exists";
        };
    }

    enum ValueType
    {
        BOOLEAN,
        LONG,
        DOUBLE,
        STRING,
    }

    record Value(ValueType type, String value)
    {
        public Value
        {
            requireNonNull(type, "type is null");
            requireNonNull(value, "value is null");
        }

        public static Value of(Object value)
        {
            requireNonNull(value, "value is null");
            if (value instanceof Boolean booleanValue) {
                return new Value(ValueType.BOOLEAN, Boolean.toString(booleanValue));
            }
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                return new Value(ValueType.LONG, value.toString());
            }
            if (value instanceof Float || value instanceof Double) {
                return new Value(ValueType.DOUBLE, value.toString());
            }
            if (value instanceof String stringValue) {
                return new Value(ValueType.STRING, stringValue);
            }
            throw new IllegalArgumentException("Unsupported remote predicate value type: " + value.getClass().getSimpleName());
        }
    }

    record And(List<ElasticsearchRemotePredicate> predicates)
            implements ElasticsearchRemotePredicate
    {
        public And
        {
            predicates = immutableNonEmptyPredicates(predicates, "AND");
        }
    }

    record Or(List<ElasticsearchRemotePredicate> predicates)
            implements ElasticsearchRemotePredicate
    {
        public Or
        {
            predicates = immutableNonEmptyPredicates(predicates, "OR");
        }
    }

    record Not(ElasticsearchRemotePredicate predicate)
            implements ElasticsearchRemotePredicate
    {
        public Not
        {
            requireNonNull(predicate, "predicate is null");
        }
    }

    record Enforced(ElasticsearchRemotePredicate predicate, Enforcement enforcement)
            implements ElasticsearchRemotePredicate
    {
        public Enforced
        {
            requireNonNull(predicate, "predicate is null");
            requireNonNull(enforcement, "enforcement is null");
            checkArgument(enforcement != Enforcement.EXACT, "EXACT predicates do not require an enforcement wrapper");
        }
    }

    record Term(String field, Value value)
            implements ElasticsearchRemotePredicate
    {
        public Term(String field, Object value)
        {
            this(field, Value.of(value));
        }

        public Term
        {
            field = requireField(field);
            requireNonNull(value, "value is null");
        }
    }

    record Terms(String field, List<Value> values)
            implements ElasticsearchRemotePredicate
    {
        public Terms(String field, Iterable<?> values)
        {
            this(field, toValues(values));
        }

        public Terms
        {
            field = requireField(field);
            values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
            checkArgument(!values.isEmpty(), "values is empty");
            checkArgument(values.stream().noneMatch(Objects::isNull), "values contains null");
        }
    }

    record Range(String field, Optional<Bound> lower, Optional<Bound> upper)
            implements ElasticsearchRemotePredicate
    {
        public Range
        {
            field = requireField(field);
            requireNonNull(lower, "lower is null");
            requireNonNull(upper, "upper is null");
            checkArgument(lower.isPresent() || upper.isPresent(), "range has no bounds");
        }
    }

    record Bound(Value value, boolean inclusive)
    {
        public Bound(Object value, boolean inclusive)
        {
            this(Value.of(value), inclusive);
        }

        public Bound
        {
            requireNonNull(value, "value is null");
        }
    }

    record Prefix(String field, String value)
            implements ElasticsearchRemotePredicate
    {
        public Prefix
        {
            field = requireField(field);
            requireNonNull(value, "value is null");
        }
    }

    record Regexp(String field, String value)
            implements ElasticsearchRemotePredicate
    {
        public Regexp
        {
            field = requireField(field);
            requireNonNull(value, "value is null");
        }
    }

    record MatchPhrase(String field, String value)
            implements ElasticsearchRemotePredicate
    {
        public MatchPhrase
        {
            field = requireField(field);
            requireNonNull(value, "value is null");
        }
    }

    record MatchPhrasePrefix(String field, String value)
            implements ElasticsearchRemotePredicate
    {
        public MatchPhrasePrefix
        {
            field = requireField(field);
            requireNonNull(value, "value is null");
        }
    }

    record Exists(String field)
            implements ElasticsearchRemotePredicate
    {
        public Exists
        {
            field = requireField(field);
        }
    }

    private static List<Value> toValues(Iterable<?> values)
    {
        requireNonNull(values, "values is null");
        ImmutableList.Builder<Value> result = ImmutableList.builder();
        for (Object value : values) {
            checkArgument(value != null, "values contains null");
            result.add(Value.of(value));
        }
        return result.build();
    }

    private static List<ElasticsearchRemotePredicate> immutableNonEmptyPredicates(List<ElasticsearchRemotePredicate> predicates, String operator)
    {
        List<ElasticsearchRemotePredicate> result = ImmutableList.copyOf(requireNonNull(predicates, "predicates is null"));
        checkArgument(!result.isEmpty(), "%s predicates is empty", operator);
        return result;
    }

    private static String requireField(String field)
    {
        requireNonNull(field, "field is null");
        checkArgument(!field.isEmpty(), "field is empty");
        return field;
    }
}
