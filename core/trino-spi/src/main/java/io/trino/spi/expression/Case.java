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
package io.trino.spi.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Represents a SQL CASE expression: {@code CASE WHEN condition THEN result [WHEN ...] [ELSE default] END}.
 * Conditions are evaluated in order, so that only the first matching result is evaluated and returned.
 * When no condition matches, the default value is evaluated and returned. A CASE without an explicit ELSE
 * clause carries a null {@link Constant} as the default value.
 */
public final class Case
        extends ConnectorExpression
{
    private final List<WhenClause> whenClauses;
    private final ConnectorExpression defaultValue;

    @JsonCreator
    public Case(
            @JsonProperty("type") Type type,
            @JsonProperty("whenClauses") List<WhenClause> whenClauses,
            @JsonProperty("defaultValue") ConnectorExpression defaultValue)
    {
        super(type);
        whenClauses = List.copyOf(requireNonNull(whenClauses, "whenClauses is null"));
        requireNonNull(defaultValue, "defaultValue is null");

        if (whenClauses.isEmpty()) {
            throw new IllegalArgumentException("whenClauses is empty");
        }
        for (WhenClause whenClause : whenClauses) {
            if (!whenClause.getCondition().getType().equals(BOOLEAN)) {
                throw new IllegalArgumentException("condition must be boolean: " + whenClause.getCondition());
            }
            if (!whenClause.getResult().getType().equals(type)) {
                throw new IllegalArgumentException("result type does not match expression type %s: %s".formatted(type, whenClause.getResult()));
            }
        }
        if (!defaultValue.getType().equals(type)) {
            throw new IllegalArgumentException("default value type does not match expression type %s: %s".formatted(type, defaultValue));
        }

        this.whenClauses = whenClauses;
        this.defaultValue = defaultValue;
    }

    @JsonProperty("whenClauses")
    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    @JsonProperty("defaultValue")
    public ConnectorExpression getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        List<ConnectorExpression> children = new ArrayList<>(whenClauses.size() * 2 + 1);
        for (WhenClause whenClause : whenClauses) {
            children.add(whenClause.getCondition());
            children.add(whenClause.getResult());
        }
        children.add(defaultValue);
        return unmodifiableList(children);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Case that = (Case) o;
        return Objects.equals(whenClauses, that.whenClauses) &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(whenClauses, defaultValue, getType());
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", Case.class.getSimpleName() + "[", "]")
                .add("whenClauses=" + whenClauses.stream()
                        .map(WhenClause::toString)
                        .collect(joining(", ", "[", "]")))
                .add("defaultValue=" + defaultValue)
                .toString();
    }

    public static final class WhenClause
    {
        private final ConnectorExpression condition;
        private final ConnectorExpression result;

        @JsonCreator
        public WhenClause(
                @JsonProperty("condition") ConnectorExpression condition,
                @JsonProperty("result") ConnectorExpression result)
        {
            this.condition = requireNonNull(condition, "condition is null");
            this.result = requireNonNull(result, "result is null");
        }

        @JsonProperty("condition")
        public ConnectorExpression getCondition()
        {
            return condition;
        }

        @JsonProperty("result")
        public ConnectorExpression getResult()
        {
            return result;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WhenClause that = (WhenClause) o;
            return Objects.equals(condition, that.condition) &&
                    Objects.equals(result, that.result);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(condition, result);
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", WhenClause.class.getSimpleName() + "[", "]")
                    .add("condition=" + condition)
                    .add("result=" + result)
                    .toString();
        }
    }
}
