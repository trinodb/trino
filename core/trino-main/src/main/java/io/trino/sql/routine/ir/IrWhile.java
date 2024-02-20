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

import io.trino.sql.relational.RowExpression;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record IrWhile(Optional<IrLabel> label, RowExpression condition, IrBlock body)
        implements IrStatement
{
    public IrWhile
    {
        requireNonNull(label, "label is null");
        requireNonNull(condition, "condition is null");
        requireNonNull(body, "body is null");
    }

    @Override
    public <C, R> R accept(IrNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitWhile(this, context);
    }
}
