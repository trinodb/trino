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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArrayConstructor
        extends Expression
{
    public static final String ARRAY_CONSTRUCTOR = "ARRAY_CONSTRUCTOR";
    private final List<Expression> values;
    private final SubqueryExpression subquery;

    public ArrayConstructor(List<Expression> values)
    {
        this(Optional.empty(), values, null);
    }

    public ArrayConstructor(SubqueryExpression subquery)
    {
        this(Optional.empty(), null, subquery);
    }

    public ArrayConstructor(NodeLocation location, List<Expression> values)
    {
        this(Optional.of(location), values, null);
    }

    public ArrayConstructor(NodeLocation location, SubqueryExpression subquery)
    {
        this(Optional.of(location), null, subquery);
    }

    private ArrayConstructor(Optional<NodeLocation> location, List<Expression> values, SubqueryExpression subquery)
    {
        super(location);
        this.subquery = subquery;
        if (subquery == null) {
            this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
        }
        else if (values == null) {
            this.values = ImmutableList.of(requireNonNull(subquery, "subquery is null"));
        }
        else {
            throw new IllegalArgumentException("both values and subquery may not have values");
        }
    }

    public boolean isSubquery()
    {
        return subquery != null;
    }

    public SubqueryExpression getSubquery()
    {
        return subquery;
    }

    public List<Expression> getValues()
    {
        return values;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitArrayConstructor(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return values;
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

        ArrayConstructor that = (ArrayConstructor) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return values.hashCode();
    }
}
