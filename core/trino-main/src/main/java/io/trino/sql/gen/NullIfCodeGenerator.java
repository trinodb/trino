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

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.instruction.LabelNode;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static java.util.Objects.requireNonNull;

public class NullIfCodeGenerator
        implements BytecodeGenerator
{
    private final RowExpression first;
    private final RowExpression second;

    private final ResolvedFunction equalsFunction;
    private final Optional<ResolvedFunction> firstCast;
    private final Optional<ResolvedFunction> secondCast;

    public NullIfCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        checkArgument(specialForm.getArguments().size() == 2);

        first = specialForm.getArguments().get(0);
        second = specialForm.getArguments().get(1);

        List<ResolvedFunction> functionDependencies = specialForm.getFunctionDependencies();
        checkArgument(functionDependencies.size() <= 3);
        equalsFunction = specialForm.getOperatorDependency(EQUAL);
        firstCast = specialForm.getCastDependency(first.getType(), equalsFunction.signature().getArgumentTypes().get(0));
        secondCast = specialForm.getCastDependency(second.getType(), equalsFunction.signature().getArgumentTypes().get(0));
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        Scope scope = generatorContext.getScope();

        LabelNode notMatch = new LabelNode("notMatch");

        // push first arg on the stack
        Variable firstValue = scope.createTempVariable(first.getType().getJavaType());
        BytecodeBlock block = new BytecodeBlock()
                .comment("check if first arg is null")
                .append(generatorContext.generate(first))
                .append(ifWasNullPopAndGoto(scope, notMatch, void.class))
                .dup(first.getType().getJavaType())
                .putVariable(firstValue);

        BytecodeNode secondValue = generatorContext.generate(second);

        // if (equal(cast(first as <common type>), cast(second as <common type>))
        BytecodeNode equalsCall = generatorContext.generateCall(
                equalsFunction,
                ImmutableList.of(
                        firstCast.map(cast -> generatorContext.generateCall(cast, ImmutableList.of(firstValue))).orElse(firstValue),
                        secondCast.map(cast -> generatorContext.generateCall(cast, ImmutableList.of(secondValue))).orElse(secondValue)));

        BytecodeBlock conditionBlock = new BytecodeBlock()
                .append(equalsCall)
                .append(BytecodeUtils.ifWasNullClearPopAndGoto(scope, notMatch, void.class, boolean.class));

        // if first and second are equal, return null
        BytecodeBlock trueBlock = new BytecodeBlock()
                .append(generatorContext.wasNull().set(constantTrue()))
                .pop(first.getType().getJavaType())
                .pushJavaDefault(first.getType().getJavaType());

        // else return first (which is still on the stack
        block.append(new IfStatement()
                .condition(conditionBlock)
                .ifTrue(trueBlock)
                .ifFalse(notMatch));

        return block;
    }
}
