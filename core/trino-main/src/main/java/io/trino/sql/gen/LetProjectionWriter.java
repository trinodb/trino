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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;

/**
 * Binds the value of a LET of row type and writes its body into the output, so a row built in the body
 * is not materialized and copied.
 */
class LetProjectionWriter
        implements ProjectionWriter
{
    @Override
    public boolean supports(Expression expression, Type type)
    {
        return expression instanceof Let && type instanceof RowType;
    }

    @Override
    public BytecodeNode write(Context context, Expression expression, Type type, BytecodeExpression output)
    {
        Let let = (Let) expression;
        Scope scope = context.scope();
        BytecodeGeneratorContext generatorContext = context.generatorContext();
        Class<?> valueJavaType = generatorContext.getCallSiteBinder().getAccessibleType(let.name().type().getJavaType());
        Variable valueTemp = scope.getOrCreateTempVariable(valueJavaType);
        Variable wasNullTemp = scope.getOrCreateTempVariable(boolean.class);

        BytecodeBlock block = new BytecodeBlock()
                .append(generatorContext.generate(let.value()))
                .putVariable(valueTemp)
                .append(wasNullTemp.set(generatorContext.wasNull()))
                .append(generatorContext.wasNull().set(constantFalse()))
                .append(context.withLet(let.name(), valueTemp, wasNullTemp).write(let.body(), type, output));

        scope.releaseTempVariableForReuse(valueTemp);
        scope.releaseTempVariableForReuse(wasNullTemp);
        return block;
    }
}
