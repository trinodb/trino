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
import io.airlift.bytecode.instruction.LabelNode;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;
import io.prestosql.sql.relational.StandardFunctionResolution;
import io.prestosql.sql.relational.VariableReferenceExpression;
import io.prestosql.sql.tree.ComparisonExpression.Operator;

import java.util.List;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static io.prestosql.sql.gen.RowExpressionCompiler.createTempVariableReferenceExpression;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.SpecialForm.Form.AND;

public class BetweenCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(ResolvedFunction resolvedFunction, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        RowExpression value = arguments.get(0);
        RowExpression min = arguments.get(1);
        RowExpression max = arguments.get(2);

        Variable firstValue = context.getScope().createTempVariable(value.getType().getJavaType());
        VariableReferenceExpression valueReference = createTempVariableReferenceExpression(firstValue, value.getType());

        StandardFunctionResolution standardFunctionResolution = new StandardFunctionResolution(context.getMetadata());
        SpecialForm newExpression = new SpecialForm(
                AND,
                BOOLEAN,
                call(
                        standardFunctionResolution.comparisonFunction(Operator.GREATER_THAN_OR_EQUAL, value.getType(), min.getType()),
                        BOOLEAN,
                        valueReference,
                        min),
                call(
                        standardFunctionResolution.comparisonFunction(Operator.LESS_THAN_OR_EQUAL, value.getType(), max.getType()),
                        BOOLEAN,
                        valueReference,
                        max));

        LabelNode done = new LabelNode("done");

        // push value arg on the stack
        BytecodeBlock block = new BytecodeBlock()
                .comment("check if value is null")
                .append(context.generate(value))
                .append(ifWasNullPopAndGoto(context.getScope(), done, boolean.class, value.getType().getJavaType()))
                .putVariable(firstValue)
                .append(context.generate(newExpression))
                .visitLabel(done);

        return block;
    }
}
