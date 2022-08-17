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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * IF(v1,v2[,v3]): CASE WHEN v1 THEN v2 [ELSE v3] END
 */
@Immutable
public class IfExpression
        extends Expression
{
    private final Expression condition;
    private final Expression trueValue;
    private final Optional<Expression> falseValue;

    @JsonCreator
    public IfExpression(
            @JsonProperty("condition") Expression condition,
            @JsonProperty("trueValue") Expression trueValue,
            @JsonProperty("falseValue") Expression falseValue)
    {
        this.condition = requireNonNull(condition, "condition is null");
        this.trueValue = requireNonNull(trueValue, "trueValue is null");
        this.falseValue = Optional.ofNullable(falseValue);
    }

    @JsonProperty
    public Expression getCondition()
    {
        return condition;
    }

    @JsonProperty
    public Expression getTrueValue()
    {
        return trueValue;
    }

    @JsonProperty
    public Optional<Expression> getFalseValue()
    {
        return falseValue;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitIfExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        falseValue.ifPresent(nodes::add);
        return nodes.add(condition)
                .add(trueValue)
                .build();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        IfExpression o = (IfExpression) obj;
        return Objects.equals(condition, o.condition) &&
                Objects.equals(trueValue, o.trueValue) &&
                Objects.equals(falseValue, o.falseValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(condition, trueValue, falseValue);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
