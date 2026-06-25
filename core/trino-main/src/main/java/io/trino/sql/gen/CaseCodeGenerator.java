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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.trino.sql.gen.ConditionalPredication.canPredicate;
import static java.util.Objects.requireNonNull;

public class CaseCodeGenerator
        implements BytecodeGenerator
{
    private final Case caseExpression;

    public CaseCodeGenerator(Case caseExpression)
    {
        this.caseExpression = requireNonNull(caseExpression, "caseExpression is null");
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        if (canPredicate(generatorContext, evaluatedExpressions())) {
            return generatePredicated(generatorContext);
        }
        return generateShortCircuit(generatorContext);
    }

    private List<Expression> evaluatedExpressions()
    {
        List<Expression> expressions = new ArrayList<>();
        for (WhenClause clause : caseExpression.whenClauses()) {
            expressions.add(clause.getOperand());
            expressions.add(clause.getResult());
        }
        expressions.add(caseExpression.defaultValue());
        return expressions;
    }

    /**
     * Branchless searched CASE for infallible, deterministic clauses: evaluate every condition, every result
     * and the default unconditionally, then keep the result of the first clause whose condition is true (a
     * NULL condition does not match), instead of short-circuiting on the first match. The value stays
     * primitive (value channel + {@code wasNull} flag), so no boxing is introduced.
     */
    private BytecodeNode generatePredicated(BytecodeGeneratorContext generatorContext)
    {
        Scope scope = generatorContext.getScope();
        Class<?> javaType = generatorContext.getCallSiteBinder().getAccessibleType(caseExpression.type().getJavaType());
        Variable wasNull = generatorContext.wasNull();
        Variable result = scope.createTempVariable(javaType);
        Variable resultNull = scope.createTempVariable(boolean.class);
        Variable matched = scope.createTempVariable(boolean.class);
        Variable conditionValue = scope.createTempVariable(boolean.class);
        Variable conditionNull = scope.createTempVariable(boolean.class);
        Variable clauseValue = scope.createTempVariable(javaType);
        Variable clauseNull = scope.createTempVariable(boolean.class);

        BytecodeBlock block = new BytecodeBlock()
                .comment("CASE (predicated)")
                .setDescription("CASE (predicated)");

        // start from the default value
        block.append(wasNull.set(constantFalse()));
        block.append(generatorContext.generate(caseExpression.defaultValue()));
        block.putVariable(result);
        block.append(resultNull.set(wasNull));
        block.append(matched.set(constantFalse()));

        for (WhenClause clause : caseExpression.whenClauses()) {
            // condition (NULL is treated as not matched)
            block.append(wasNull.set(constantFalse()));
            block.append(generatorContext.generate(clause.getOperand()));
            block.putVariable(conditionValue);
            block.append(conditionNull.set(wasNull));
            // result, evaluated eagerly and independently of the condition
            block.append(wasNull.set(constantFalse()));
            block.append(generatorContext.generate(clause.getResult()));
            block.putVariable(clauseValue);
            block.append(clauseNull.set(wasNull));
            // keep the first matching clause
            block.append(new IfStatement()
                    .condition(and(not(matched), and(not(conditionNull), conditionValue)))
                    .ifTrue(new BytecodeBlock()
                            .append(result.set(clauseValue))
                            .append(resultNull.set(clauseNull))
                            .append(matched.set(constantTrue()))));
        }

        block.append(wasNull.set(resultNull));
        block.append(result);

        return block;
    }

    private BytecodeNode generateShortCircuit(BytecodeGeneratorContext generatorContext)
    {
        // Generate nested IF bytecode: IF(cond1, val1, IF(cond2, val2, ... default))
        BytecodeNode result = generatorContext.generate(caseExpression.defaultValue());

        for (WhenClause clause : caseExpression.whenClauses().reversed()) {
            Variable wasNull = generatorContext.wasNull();
            BytecodeBlock conditionBlock = new BytecodeBlock()
                    .append(generatorContext.generate(clause.getOperand()))
                    .comment("... and condition value was not null")
                    .append(wasNull)
                    .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)
                    .invokeStatic(CompilerOperations.class, "and", boolean.class, boolean.class, boolean.class)
                    .append(wasNull.set(constantFalse()));

            result = new IfStatement()
                    .condition(conditionBlock)
                    .ifTrue(generatorContext.generate(clause.getResult()))
                    .ifFalse(result);
        }
        return result;
    }
}
