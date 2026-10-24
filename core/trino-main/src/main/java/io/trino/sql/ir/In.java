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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrUtils.validateType;

/// SQL equality against the elements of an array expression. The array's element
/// type must match the value's type. A null array produces null; an empty array
/// produces false, including for a null value. Otherwise a match produces true,
/// and no match produces null if any comparison is indeterminate, or false.
///
/// Array constructors and non-null array constants can use specialized IN
/// evaluation without materializing a new array. Other array expressions are
/// evaluated once at runtime. This is an internal IR representation; it does not
/// add SQL syntax or change the semantics of SQL IN lists.
///
/// The array expression is the sole representation of the candidates. Consumers
/// that need known elements inspect it without retaining a second representation
/// in this node.
@JsonSerialize
public record In(Expression value, Expression valueList)
        implements Expression
{
    public In
    {
        validateType(new ArrayType(value.type()), valueList);
    }

    @Override
    public Type type()
    {
        return BOOLEAN;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitIn(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of(value, valueList);
    }

    @Override
    public String toString()
    {
        return "$in(%s, %s)".formatted(value, valueList);
    }
}
