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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.IrUtils.validateType;

@JsonSerialize
public record FieldReference(Expression base, Expression index)
        implements Expression
{
    public FieldReference
    {
        if (!(base.type() instanceof RowType rowType)) {
            throw new IllegalArgumentException("Expected 'row' type but found '%s' for expression: %s".formatted(base.type(), base));
        }

        validateType(INTEGER, index);
        int field = (int) (long) ((Constant) index).value() - 1;
        checkArgument(field < rowType.getFields().size(), "Expected 'row' type to have at least %s fields, but has: %s", field + 1, rowType.getFields().size());
    }

    @Override
    public Type type()
    {
        int field = (int) (long) ((Constant) index).value() - 1;
        return ((RowType) base.type()).getFields().get(field).getType();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitFieldReference(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of(base, index);
    }
}
