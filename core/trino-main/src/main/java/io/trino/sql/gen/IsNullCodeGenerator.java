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
import io.airlift.bytecode.Variable;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

public class IsNullCodeGenerator
        implements BytecodeGenerator
{
    private final RowExpression argument;

    public IsNullCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        checkArgument(specialForm.arguments().size() == 1);
        argument = specialForm.arguments().get(0);
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        if (argument.type().equals(UNKNOWN)) {
            return loadBoolean(true);
        }

        BytecodeNode value = generatorContext.generate(argument);

        // evaluate the expression, pop the produced value, and load the null flag
        Variable wasNull = generatorContext.wasNull();
        BytecodeBlock block = new BytecodeBlock()
                .comment("is null")
                .append(value)
                .pop(argument.type().getJavaType())
                .append(wasNull);

        // clear the null flag
        block.append(wasNull.set(constantFalse()));

        return block;
    }
}
