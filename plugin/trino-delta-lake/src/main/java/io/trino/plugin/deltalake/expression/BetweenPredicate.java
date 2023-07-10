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
package io.trino.plugin.deltalake.expression;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BetweenPredicate
        extends SparkExpression
{
    public enum Operator
    {
        BETWEEN("BETWEEN"),
        NOT_BETWEEN("NOT BETWEEN");

        private final String value;

        Operator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
    }

    private final Operator operator;
    private final SparkExpression value;
    private final SparkExpression min;
    private final SparkExpression max;

    public BetweenPredicate(Operator operator, SparkExpression value, SparkExpression min, SparkExpression max)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.value = requireNonNull(value, "value is null");
        this.min = requireNonNull(min, "min is null");
        this.max = requireNonNull(max, "max is null");
    }

    public Operator getOperator()
    {
        return operator;
    }

    public SparkExpression getValue()
    {
        return value;
    }

    public SparkExpression getMin()
    {
        return min;
    }

    public SparkExpression getMax()
    {
        return max;
    }

    @Override
    <R, C> R accept(SparkExpressionTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitBetweenExpression(this, context);
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
        BetweenPredicate that = (BetweenPredicate) o;
        return operator == that.operator &&
                Objects.equals(value, that.value) &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, value, min, max);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("operator", operator)
                .add("value", value)
                .add("min", min)
                .add("max", max)
                .toString();
    }
}
