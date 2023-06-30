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
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.Type;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;
import java.util.Optional;

import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.util.Objects.requireNonNull;

public class RowConstructorCodeGenerator
        implements BytecodeGenerator
{
    private final Type rowType;
    private final List<RowExpression> arguments;

    public RowConstructorCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        rowType = specialForm.getType();
        arguments = specialForm.getArguments();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType);
        CallSiteBinder binder = context.getCallSiteBinder();
        Scope scope = context.getScope();
        List<Type> types = rowType.getTypeParameters();

        Variable fieldBlocks = scope.createTempVariable(Block[].class);
        block.append(fieldBlocks.set(newArray(type(Block[].class), arguments.size())));

        Variable blockBuilder = scope.createTempVariable(BlockBuilder.class);
        for (int i = 0; i < arguments.size(); ++i) {
            Type fieldType = types.get(i);
            Variable field = scope.createTempVariable(fieldType.getJavaType());

            block.append(blockBuilder.set(constantType(binder, fieldType).invoke(
                    "createBlockBuilder",
                    BlockBuilder.class,
                    constantNull(BlockBuilderStatus.class),
                    constantInt(1))));

            block.comment("Clean wasNull and Generate + " + i + "-th field of row");
            block.append(context.wasNull().set(constantFalse()));
            block.append(context.generate(arguments.get(i)));
            block.putVariable(field);
            block.append(new IfStatement()
                    .condition(context.wasNull())
                    .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, fieldType).writeValue(blockBuilder, field).pop()));

            block.append(fieldBlocks.setElement(i, blockBuilder.invoke("build", Block.class)));
        }

        BytecodeExpression rowBlock = invokeStatic(
                RowBlock.class,
                "fromFieldBlocks",
                Block.class,
                constantInt(1),
                invokeStatic(Optional.class, "empty", Optional.class),
                fieldBlocks);

        block.append(constantType(binder, rowType).invoke("getObject", Object.class, rowBlock, constantInt(0))
                .cast(Block.class));
        block.append(context.wasNull().set(constantFalse()));
        return block;
    }
}
