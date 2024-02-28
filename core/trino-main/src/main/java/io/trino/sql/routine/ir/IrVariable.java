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
package io.trino.sql.routine.ir;

import io.trino.spi.type.Type;
import io.trino.sql.relational.RowExpression;

import static java.util.Objects.requireNonNull;

public record IrVariable(int field, Type type, RowExpression defaultValue)
        implements IrNode
{
    public IrVariable
    {
        requireNonNull(type, "type is null");
        requireNonNull(defaultValue, "value is null");
    }

    @Override
    public <C, R> R accept(IrNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitVariable(this, context);
    }
}
