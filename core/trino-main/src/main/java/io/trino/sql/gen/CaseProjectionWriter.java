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

import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;

/**
 * Writes each branch of a CASE (and so of an IF) of row type into the output, so a row built in a branch
 * is not materialized and copied, and the null branch only appends a null.
 */
class CaseProjectionWriter
        implements ProjectionWriter
{
    @Override
    public boolean supports(Expression expression, Type type)
    {
        return expression instanceof Case && type instanceof RowType;
    }

    @Override
    public BytecodeNode write(Context context, Expression expression, Type type, BytecodeExpression output)
    {
        Case caseExpression = (Case) expression;
        BytecodeNode result = context.write(caseExpression.defaultValue(), type, output);
        for (WhenClause clause : caseExpression.whenClauses().reversed()) {
            result = new IfStatement()
                    .condition(ExpressionBytecodeCompiler.caseCondition(context.generatorContext(), clause))
                    .ifTrue(context.write(clause.result(), type, output))
                    .ifFalse(result);
        }
        return result;
    }
}
