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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;

import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static io.trino.sql.gen.ExpressionBytecodeCompiler.createTempReference;
import static io.trino.sql.ir.IrExpressions.call;
import static java.util.Objects.requireNonNull;

public class BetweenCodeGenerator
        implements BytecodeGenerator
{
    private final Expression value;
    private final Expression min;
    private final Expression max;

    private final ResolvedFunction lessThanOrEqual;

    public BetweenCodeGenerator(Between between, Metadata metadata)
    {
        requireNonNull(between, "between is null");
        value = between.value();
        min = between.min();
        max = between.max();

        lessThanOrEqual = metadata.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(value.type(), max.type()));
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        Class<?> valueJavaType = context.getCallSiteBinder().getAccessibleType(value.type().getJavaType());
        Variable firstValue = context.getScope().getOrCreateTempVariable(valueJavaType);
        Reference valueReference = createTempReference(firstValue, value.type());

        Logical newExpression = new Logical(
                Logical.Operator.AND,
                ImmutableList.of(
                        call(lessThanOrEqual, min, valueReference),
                        call(lessThanOrEqual, valueReference, max)));

        LabelNode done = new LabelNode("done");

        // push value arg on the stack
        BytecodeBlock block = new BytecodeBlock()
                .comment("check if value is null")
                .append(context.generate(value))
                .append(ifWasNullPopAndGoto(context.getScope(), done, boolean.class, valueJavaType))
                .putVariable(firstValue)
                .append(context.generate(newExpression))
                .visitLabel(done);

        context.getScope().releaseTempVariableForReuse(firstValue);

        return block;
    }
}
