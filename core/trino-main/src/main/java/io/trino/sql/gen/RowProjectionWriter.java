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
package io.trino.sql.gen;

import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RowEntryBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;

import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.trino.sql.gen.RowConstructorCodeGenerator.MEGAMORPHIC_FIELD_COUNT;

/**
 * Writes the fields of a ROW constructor into the field builders of the output.
 */
class RowProjectionWriter
        implements ProjectionWriter
{
    @Override
    public boolean supports(Expression expression, Type type)
    {
        return expression instanceof Row row && row.items().size() <= MEGAMORPHIC_FIELD_COUNT;
    }

    @Override
    public BytecodeNode write(Context context, Expression expression, Type type, BytecodeExpression output)
    {
        Row row = (Row) expression;
        Scope scope = context.scope();
        // A field failure can leave an open entry with uneven field counts. GeneratedPageProjection
        // creates a fresh output builder on every project() call, including dictionary retries.
        List<Type> fieldTypes = ((RowType) type).getFieldTypes();
        Variable entry = scope.getOrCreateTempVariable(RowEntryBuilder.class);
        BytecodeBlock block = new BytecodeBlock();
        block.append(entry.set(output.cast(RowBlockBuilder.class).invoke("buildEntry", RowEntryBuilder.class)));
        for (int field = 0; field < row.items().size(); field++) {
            block.append(context.write(row.items().get(field), fieldTypes.get(field), entry.invoke("getFieldBuilder", BlockBuilder.class, constantInt(field))));
        }
        block.append(entry.invoke("build", void.class));
        scope.releaseTempVariableForReuse(entry);
        return block;
    }
}
