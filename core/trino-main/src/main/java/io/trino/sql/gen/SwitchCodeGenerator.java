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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.trino.spi.function.OperatorType.EQUAL;
import static java.util.Objects.requireNonNull;

public class SwitchCodeGenerator
        implements BytecodeGenerator
{
    private final Expression value;
    private final List<WhenClause> whenClauses;
    private final Expression defaultValue;
    private final List<ResolvedFunction> equalsFunctions;

    public SwitchCodeGenerator(Switch switchExpression, Metadata metadata)
    {
        requireNonNull(switchExpression, "switchExpression is null");
        value = switchExpression.operand();
        whenClauses = switchExpression.whenClauses();
        defaultValue = switchExpression.defaultValue();

        equalsFunctions = whenClauses.stream()
                .map(clause -> metadata.resolveOperator(EQUAL, ImmutableList.of(value.type(), clause.getOperand().type())))
                .collect(toImmutableList());
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

        BytecodeNode elseValue = generatorContext.generate(defaultValue);

        // determine the type of the value and result
        Class<?> valueType = value.type().getJavaType();

        // evaluate the value and store it in a variable
        LabelNode nullValue = new LabelNode("nullCondition");
        Variable tempVariable = scope.getOrCreateTempVariable(valueType);
        BytecodeBlock block = new BytecodeBlock()
                .append(valueBytecode)
                .append(BytecodeUtils.ifWasNullClearPopAndGoto(scope, nullValue, void.class, valueType))
                .putVariable(tempVariable);

        BytecodeNode getTempVariableNode = VariableInstruction.loadVariable(tempVariable);

        // build the statements
        elseValue = new BytecodeBlock().visitLabel(nullValue).append(elseValue);
        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (int i = whenClauses.size() - 1; i >= 0; i--) {
            WhenClause clause = whenClauses.get(i);
            Expression operand = clause.getOperand();
            Expression result = clause.getResult();

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

        block.append(elseValue);
        scope.releaseTempVariableForReuse(tempVariable);
        return block;
    }
}
