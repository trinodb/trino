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
import io.airlift.bytecode.instruction.VariableInstruction;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.trino.sql.relational.SpecialForm.Form.WHEN;
import static java.util.Objects.requireNonNull;

public class SwitchCodeGenerator
        implements BytecodeGenerator
{
    private final Type returnType;
    private final RowExpression value;
    private final List<SpecialForm> whenClauses;
    private final Optional<RowExpression> elseValue;
    private final List<ResolvedFunction> equalsFunctions;

    public SwitchCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        returnType = specialForm.type();
        List<RowExpression> arguments = specialForm.arguments();
        value = arguments.getFirst();

        RowExpression last = arguments.getLast();
        if (last instanceof SpecialForm && ((SpecialForm) last).form() == WHEN) {
            whenClauses = arguments.subList(1, arguments.size()).stream()
                    .map(SpecialForm.class::cast)
                    .collect(toImmutableList());
            elseValue = Optional.empty();
        }
        else {
            whenClauses = arguments.subList(1, arguments.size() - 1).stream()
                    .map(SpecialForm.class::cast)
                    .collect(toImmutableList());
            elseValue = Optional.of(last);
        }
        checkArgument(whenClauses.stream()
                .map(SpecialForm::form)
                .allMatch(WHEN::equals));

        equalsFunctions = ImmutableList.copyOf(specialForm.functionDependencies());
        checkArgument(equalsFunctions.size() == whenClauses.size());
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        // TODO: compile as
        /*
            hashCode = hashCode(<value>)

            // all constant expressions before a non-constant
            switch (hashCode) {
                case ...:
                    if (<value> == <constant1>) {
                       ...
                    }
                    else if (<value> == <constant2>) {
                       ...
                    }
                    else if (...) {
                    }
                case ...:
                    ...
            }

            if (<value> == <non-constant1>) {
                ...
            }
            else if (<value> == <non-constant2>) {
                ...
            }
            ...

            // repeat with next sequence of constant expressions
         */

        Scope scope = generatorContext.getScope();

        // process value, else, and all when clauses
        BytecodeNode valueBytecode = generatorContext.generate(value);

        BytecodeNode elseValue;
        if (this.elseValue.isEmpty()) {
            elseValue = new BytecodeBlock()
                    .append(generatorContext.wasNull().set(constantTrue()))
                    .pushJavaDefault(returnType.getJavaType());
        }
        else {
            elseValue = generatorContext.generate(this.elseValue.get());
        }

        // determine the type of the value and result
        Class<?> valueType = value.type().getJavaType();

        // evaluate the value and store it in a variable
        LabelNode nullValue = new LabelNode("nullCondition");
        Variable tempVariable = scope.createTempVariable(valueType);
        BytecodeBlock block = new BytecodeBlock()
                .append(valueBytecode)
                .append(BytecodeUtils.ifWasNullClearPopAndGoto(scope, nullValue, void.class, valueType))
                .putVariable(tempVariable);

        BytecodeNode getTempVariableNode = VariableInstruction.loadVariable(tempVariable);

        // build the statements
        elseValue = new BytecodeBlock().visitLabel(nullValue).append(elseValue);
        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (int i = whenClauses.size() - 1; i >= 0; i--) {
            SpecialForm clause = whenClauses.get(i);
            RowExpression operand = clause.arguments().get(0);
            RowExpression result = clause.arguments().get(1);

            // call equals(value, operand)

            // TODO: what if operand is null? It seems that the call will return "null" (which is cleared below)
            // and the code only does the right thing because the value in the stack for that scenario is
            // Java's default for boolean == false
            // This code should probably be checking for wasNull after the call and "failing" the equality
            // check if wasNull is true
            BytecodeNode equalsCall = generatorContext.generateCall(
                    equalsFunctions.get(i),
                    ImmutableList.of(generatorContext.generate(operand), getTempVariableNode));

            BytecodeBlock condition = new BytecodeBlock()
                    .append(equalsCall)
                    .append(generatorContext.wasNull().set(constantFalse()));

            elseValue = new IfStatement("when")
                    .condition(condition)
                    .ifTrue(generatorContext.generate(result))
                    .ifFalse(elseValue);
        }

        return block.append(elseValue);
    }
}
