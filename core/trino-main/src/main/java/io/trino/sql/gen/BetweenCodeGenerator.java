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
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.instruction.LabelNode;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static io.trino.sql.gen.RowExpressionCompiler.createTempVariableReferenceExpression;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static java.util.Objects.requireNonNull;

public class BetweenCodeGenerator
        implements BytecodeGenerator
{
    private final RowExpression value;
    private final RowExpression min;
    private final RowExpression max;

    private final ResolvedFunction lessThanOrEqual;

    public BetweenCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        List<RowExpression> arguments = specialForm.arguments();
        checkArgument(arguments.size() == 3);
        value = arguments.get(0);
        min = arguments.get(1);
        max = arguments.get(2);

        checkArgument(specialForm.functionDependencies().size() == 1);
        lessThanOrEqual = specialForm.getOperatorDependency(LESS_THAN_OR_EQUAL);
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        Variable firstValue = context.getScope().createTempVariable(value.type().getJavaType());
        VariableReferenceExpression valueReference = createTempVariableReferenceExpression(firstValue, value.type());

        SpecialForm newExpression = new SpecialForm(
                AND,
                BOOLEAN,
                ImmutableList.of(call(lessThanOrEqual, min, valueReference), call(lessThanOrEqual, valueReference, max)),
                ImmutableList.of());

        LabelNode done = new LabelNode("done");

        // push value arg on the stack
        BytecodeBlock block = new BytecodeBlock()
                .comment("check if value is null")
                .append(context.generate(value))
                .append(ifWasNullPopAndGoto(context.getScope(), done, boolean.class, value.type().getJavaType()))
                .putVariable(firstValue)
                .append(context.generate(newExpression))
                .visitLabel(done);

        return block;
    }
}
