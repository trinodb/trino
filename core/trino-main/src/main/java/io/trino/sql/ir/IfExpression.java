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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * IF(v1,v2[,v3]): CASE WHEN v1 THEN v2 [ELSE v3] END
 *
 * TODO: normalize representation by making the ELSE branch always required
 */
public final class IfExpression
        extends Expression
{
    private final Expression condition;
    private final Expression trueValue;
    private final Optional<Expression> falseValue;

    public IfExpression(Expression condition, Expression trueValue)
    {
        this(condition, trueValue, Optional.empty());
    }

    @JsonCreator
    public IfExpression(Expression condition, Expression trueValue, Optional<Expression> falseValue)
    {
        this.condition = requireNonNull(condition, "condition is null");
        this.trueValue = requireNonNull(trueValue, "trueValue is null");
        this.falseValue = requireNonNull(falseValue, "falseValue is null");
    }

    public IfExpression(Expression condition, Expression trueValue, Expression falseValue)
    {
        this(condition, trueValue, Optional.ofNullable(falseValue));
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
    public List<? extends Expression> getChildren()
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.<Expression>builder()
                .add(condition)
                .add(trueValue);

        falseValue.ifPresent(builder::add);

        return builder.build();
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
    public String toString()
    {
        return "If(%s, %s%s)".formatted(
                condition,
                trueValue,
                falseValue.map(value -> ", " + value).orElse(""));
    }
}
