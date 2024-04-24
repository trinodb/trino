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
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.Type;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DereferenceCodeGenerator
        implements BytecodeGenerator
{
    private final Type returnType;
    private final RowExpression base;
    private final int index;

    public DereferenceCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        returnType = specialForm.getType();
        checkArgument(specialForm.getArguments().size() == 2);
        base = specialForm.getArguments().get(0);
        index = toIntExact((long) ((ConstantExpression) specialForm.getArguments().get(1)).getValue());
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generator)
    {
        CallSiteBinder callSiteBinder = generator.getCallSiteBinder();

        BytecodeBlock block = new BytecodeBlock().comment("DEREFERENCE").setDescription("DEREFERENCE");
        Variable wasNull = generator.wasNull();
        Variable row = generator.getScope().createTempVariable(SqlRow.class);

        // clear the wasNull flag before evaluating the row value
        block.putVariable(wasNull, false);
        block.append(generator.generate(base)).putVariable(row);

        IfStatement ifRowBlockIsNull = new IfStatement("if row block is null...")
                .condition(wasNull);

        Class<?> javaType = returnType.getJavaType();
        LabelNode end = new LabelNode("end");
        ifRowBlockIsNull.ifTrue()
                .comment("if row block is null, push null to the stack and goto 'end' label (return)")
                .putVariable(wasNull, true)
                .pushJavaDefault(javaType)
                .gotoLabel(end);

        block.append(ifRowBlockIsNull);

        IfStatement ifFieldIsNull = new IfStatement("if row field is null...");
        ifFieldIsNull.condition(row.invoke("getRawFieldBlock", Block.class, constantInt(index)).invoke("isNull", boolean.class, row.invoke("getRawIndex", int.class)));

        ifFieldIsNull.ifTrue()
                .comment("if the field is null, push null to stack")
                .putVariable(wasNull, true)
                .pushJavaDefault(javaType);

        BytecodeExpression value = constantType(callSiteBinder, returnType).getValue(row.invoke("getRawFieldBlock", Block.class, constantInt(index)), row.invoke("getRawIndex", int.class));

        ifFieldIsNull.ifFalse()
                .comment("otherwise call type.getTYPE(row, index)")
                .append(value)
                .putVariable(wasNull, false);

        block.append(ifFieldIsNull)
                .visitLabel(end);

        return block;
    }
}
