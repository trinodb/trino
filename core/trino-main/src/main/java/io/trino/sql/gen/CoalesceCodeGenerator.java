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
import io.airlift.bytecode.control.IfStatement;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.defaultValue;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.trino.sql.gen.ConditionalPredication.canPredicate;
import static java.util.Objects.requireNonNull;

public class CoalesceCodeGenerator
        implements BytecodeGenerator
{
    private final Type returnType;
    private final List<Expression> arguments;

    public CoalesceCodeGenerator(Coalesce coalesce)
    {
        requireNonNull(coalesce, "coalesce is null");
        returnType = coalesce.type();
        arguments = coalesce.operands();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        if (canPredicate(generatorContext, arguments)) {
            return generatePredicated(generatorContext);
        }
        return generateShortCircuit(generatorContext);
    }

    /**
     * Branchless coalesce for infallible, deterministic operands: evaluate every operand unconditionally and
     * keep the first non-null one, instead of short-circuiting on the first non-null. The value stays
     * primitive ({@code result} value channel + {@code wasNull} flag), so no boxing is introduced.
     */
    private BytecodeNode generatePredicated(BytecodeGeneratorContext generatorContext)
    {
        Scope scope = generatorContext.getScope();
        Class<?> javaType = generatorContext.getCallSiteBinder().getAccessibleType(returnType.getJavaType());
        Variable wasNull = generatorContext.wasNull();
        Variable result = scope.createTempVariable(javaType);
        Variable found = scope.createTempVariable(boolean.class);
        Variable value = scope.createTempVariable(javaType);

        BytecodeBlock block = new BytecodeBlock()
                .comment("COALESCE (predicated)")
                .setDescription("COALESCE (predicated)");

        block.append(result.set(defaultValue(javaType)));
        block.append(found.set(constantFalse()));

        for (Expression argument : arguments) {
            block.append(wasNull.set(constantFalse()));
            block.append(generatorContext.generate(argument)); // leaves the value on the stack, sets wasNull
            block.putVariable(value);
            // keep the first non-null operand: result = (!found && !wasNull) ? value : result
            block.append(new IfStatement()
                    .condition(and(not(found), not(wasNull)))
                    .ifTrue(new BytecodeBlock()
                            .append(result.set(value))
                            .append(found.set(constantTrue()))));
        }

        block.append(wasNull.set(not(found)));
        block.append(result);

        return block;
    }

    private BytecodeNode generateShortCircuit(BytecodeGeneratorContext generatorContext)
    {
        Class<?> returnJavaType = generatorContext.getCallSiteBinder().getAccessibleType(returnType.getJavaType());

        List<BytecodeNode> operands = new ArrayList<>();
        for (Expression expression : arguments) {
            operands.add(generatorContext.generate(expression));
        }

        Variable wasNull = generatorContext.wasNull();
        BytecodeNode nullValue = new BytecodeBlock()
                .append(wasNull.set(constantTrue()))
                .pushJavaDefault(returnJavaType);

        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (BytecodeNode operand : operands.reversed()) {
            IfStatement ifStatement = new IfStatement();

            ifStatement.condition()
                    .append(operand)
                    .append(wasNull);

            // if value was null, pop the null value, clear the null flag, and process the next operand
            ifStatement.ifTrue()
                    .pop(returnJavaType)
                    .append(wasNull.set(constantFalse()))
                    .append(nullValue);

            nullValue = ifStatement;
        }

        return nullValue;
    }
}
