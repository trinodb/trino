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
package io.trino.plugin.pinot.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class OrderByExpression
{
    private final String expression;
    private final boolean asc;

    @JsonCreator
    public OrderByExpression(
            @JsonProperty("expression") String expression,
            @JsonProperty("asc") boolean asc)
    {
        this.expression = requireNonNull(expression, "column is null");
        this.asc = asc;
    }

    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    @JsonProperty
    public boolean isAsc()
    {
        return asc;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof OrderByExpression)) {
            return false;
        }
        OrderByExpression that = (OrderByExpression) other;
        return expression.equals(that.expression) &&
                asc == that.asc;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, asc);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("asc", asc)
                .toString();
    }
}
