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
package io.prestosql.sql.gen;

import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static java.util.Objects.requireNonNull;

public class IfCodeGenerator
        implements BytecodeGenerator
{
    private final RowExpression condition;
    private final RowExpression trueValue;
    private final RowExpression falseValue;

    public IfCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        List<RowExpression> arguments = specialForm.getArguments();
        checkArgument(arguments.size() == 3);
        condition = arguments.get(0);
        trueValue = arguments.get(1);
        falseValue = arguments.get(2);
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        Variable wasNull = context.wasNull();
        BytecodeBlock conditionBlock = new BytecodeBlock()
                .append(context.generate(condition))
                .comment("... and condition value was not null")
                .append(wasNull)
                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)
                .invokeStatic(CompilerOperations.class, "and", boolean.class, boolean.class, boolean.class)
                .append(wasNull.set(constantFalse()));

        return new IfStatement()
                .condition(conditionBlock)
                .ifTrue(context.generate(trueValue))
                .ifFalse(context.generate(falseValue));
    }
}
