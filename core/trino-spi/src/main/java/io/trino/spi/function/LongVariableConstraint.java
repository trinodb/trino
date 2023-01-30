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
package io.trino.spi.function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Experimental;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Experimental(eta = "2022-10-31")
public class LongVariableConstraint
{
    private final String name;
    private final String expression;

    LongVariableConstraint(String name, String expression)
    {
        this.name = requireNonNull(name, "name is null");
        this.expression = requireNonNull(expression, "expression is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    @Override
    public String toString()
    {
        return name + ":" + expression;
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
        LongVariableConstraint that = (LongVariableConstraint) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, expression);
    }

    /**
     * This method is only visible for JSON deserialization.
     *
     * @deprecated use builder
     */
    @Deprecated
    @JsonCreator
    public static LongVariableConstraint fromJson(
            @JsonProperty("name") String name,
            @JsonProperty("expression") String expression)
    {
        return new LongVariableConstraint(name, expression);
    }
}
