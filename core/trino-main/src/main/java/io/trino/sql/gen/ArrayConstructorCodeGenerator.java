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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;

public class ArrayConstructorCodeGenerator
        implements BytecodeGenerator
{
    private final Type elementType;
    private final List<RowExpression> elements;

    public ArrayConstructorCodeGenerator(SpecialForm specialForm)
    {
        elementType = ((ArrayType) specialForm.type()).getElementType();
        elements = specialForm.arguments();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        CallSiteBinder binder = context.getCallSiteBinder();
        Scope scope = context.getScope();

        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for array(%s)".formatted(elementType));

        Variable blockBuilder = scope.createTempVariable(BlockBuilder.class);
        block.append(blockBuilder.set(constantType(binder, elementType).invoke("createBlockBuilder", BlockBuilder.class, constantNull(BlockBuilderStatus.class), constantInt(elements.size()))));

        Variable element = scope.createTempVariable(elementType.getJavaType());

        for (RowExpression item : elements) {
            block.append(context.wasNull().set(constantFalse()));
            block.append(context.generate(item));
            block.putVariable(element);
            block.append(new IfStatement()
                    .condition(context.wasNull())
                    .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, elementType).writeValue(blockBuilder, element).pop()));
        }

        block.append(blockBuilder.invoke("build", Block.class));
        block.append(context.wasNull().set(constantFalse()));

        return block;
    }
}
