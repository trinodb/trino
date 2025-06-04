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
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowConstructorCodeGenerator
        implements BytecodeGenerator
{
    private final Type rowType;
    private final List<RowExpression> arguments;
    // Arbitrary value chosen to balance the code size vs performance trade off. Not perf tested.
    private static final int MEGAMORPHIC_FIELD_COUNT = 64;

    // number of fields to initialize in a single method for large rows
    private static final int LARGE_ROW_BATCH_SIZE = 100;

    public RowConstructorCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        rowType = specialForm.type();
        arguments = specialForm.arguments();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        if (arguments.size() > MEGAMORPHIC_FIELD_COUNT) {
            return generateExpressionForLargeRows(context);
        }

        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType);
        CallSiteBinder binder = context.getCallSiteBinder();
        Scope scope = context.getScope();
        List<Type> types = rowType.getTypeParameters();

        Variable fieldBlocks = scope.getOrCreateTempVariable(Block[].class);
        block.append(fieldBlocks.set(newArray(type(Block[].class), arguments.size())));

        Variable blockBuilder = scope.getOrCreateTempVariable(BlockBuilder.class);
        for (int i = 0; i < arguments.size(); ++i) {
            Type fieldType = types.get(i);

            block.append(blockBuilder.set(constantType(binder, fieldType).invoke(
                    "createBlockBuilder",
                    BlockBuilder.class,
                    constantNull(BlockBuilderStatus.class),
                    constantInt(1))));

            block.comment("Clean wasNull and Generate + " + i + "-th field of row");
            block.append(context.wasNull().set(constantFalse()));
            block.append(context.generate(arguments.get(i)));
            Variable field = scope.getOrCreateTempVariable(fieldType.getJavaType());
            block.putVariable(field);
            block.append(new IfStatement()
                    .condition(context.wasNull())
                    .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, fieldType).writeValue(blockBuilder, field).pop()));
            scope.releaseTempVariableForReuse(field);

            block.append(fieldBlocks.setElement(i, blockBuilder.invoke("build", Block.class)));
        }
        scope.releaseTempVariableForReuse(blockBuilder);

        block.append(newInstance(SqlRow.class, constantInt(0), fieldBlocks));
        scope.releaseTempVariableForReuse(fieldBlocks);
        block.append(context.wasNull().set(constantFalse()));
        return block;
    }

    // Avoids inline BlockBuilder and Block creation logic for each field which reduces the generated code size
    // for RowTypes with many fields significantly, but does so at the cost of virtual method dispatch
    private BytecodeNode generateExpressionForLargeRows(BytecodeGeneratorContext context)
    {
        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType);
        CallSiteBinder binder = context.getCallSiteBinder();
        Scope scope = context.getScope();

        Variable fieldBuilders = scope.getOrCreateTempVariable(BlockBuilder[].class);
        block.append(fieldBuilders.set(invokeStatic(RowConstructorCodeGenerator.class, "createFieldBlockBuildersForSingleRow", BlockBuilder[].class, constantType(binder, rowType))));

        Variable blockBuilder = scope.getOrCreateTempVariable(BlockBuilder.class);
        for (int i = 0; i < arguments.size(); i += LARGE_ROW_BATCH_SIZE) {
            MethodDefinition partialRowConstructor = generatePartialRowConstructor(i, Math.min(i + LARGE_ROW_BATCH_SIZE, arguments.size()), context);
            block.getVariable(scope.getThis());
            for (Parameter argument : context.getContextArguments()) {
                block.getVariable(argument);
            }
            block.getVariable(fieldBuilders);
            block.invokeVirtual(partialRowConstructor);
        }
        scope.releaseTempVariableForReuse(blockBuilder);

        block.append(invokeStatic(RowConstructorCodeGenerator.class, "createSqlRowFromFieldBuildersForSingleRow", SqlRow.class, fieldBuilders));
        scope.releaseTempVariableForReuse(fieldBuilders);
        block.append(context.wasNull().set(constantFalse()));
        return block;
    }

    private MethodDefinition generatePartialRowConstructor(int start, int end, BytecodeGeneratorContext parentContext)
    {
        ClassDefinition classDefinition = parentContext.getClassDefinition();
        CallSiteBinder binder = parentContext.getCallSiteBinder();

        Parameter fieldBuilders = arg("fieldBuilders", BlockBuilder[].class);

        List<Parameter> parameters = new ArrayList<>(parentContext.getContextArguments());
        parameters.add(fieldBuilders);

        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PUBLIC),
                "partialRowConstructor" + System.identityHashCode(this) + "_" + start,
                type(void.class),
                parameters);

        Scope scope = methodDefinition.getScope();
        BytecodeBlock block = methodDefinition.getBody();
        scope.declareVariable("wasNull", block, constantFalse());

        BytecodeGeneratorContext context = new BytecodeGeneratorContext(parentContext.getRowExpressionCompiler(), scope, binder, parentContext.getCachedInstanceBinder(), parentContext.getFunctionManager(), classDefinition, parentContext.getContextArguments());
        Variable blockBuilder = scope.getOrCreateTempVariable(BlockBuilder.class);
        List<Type> types = rowType.getTypeParameters();
        for (int i = start; i < end; i++) {
            Type fieldType = types.get(i);

            block.append(blockBuilder.set(fieldBuilders.getElement(constantInt(i))));

            block.comment("Clean wasNull and Generate + " + i + "-th field of row");

            block.append(context.wasNull().set(constantFalse()));
            block.append(context.generate(arguments.get(i)));
            Variable field = scope.getOrCreateTempVariable(fieldType.getJavaType());
            block.putVariable(field);
            block.append(new IfStatement()
                    .condition(context.wasNull())
                    .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, fieldType).writeValue(blockBuilder, field).pop()));
            scope.releaseTempVariableForReuse(field);
        }

        block.ret();
        return methodDefinition;
    }

    @UsedByGeneratedCode
    public static BlockBuilder[] createFieldBlockBuildersForSingleRow(Type rowType)
    {
        if (!(rowType instanceof RowType)) {
            throw new IllegalArgumentException("Not a row type: " + rowType);
        }
        List<Type> fieldTypes = rowType.getTypeParameters();
        BlockBuilder[] fieldBlockBuilders = new BlockBuilder[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldBlockBuilders[i] = fieldTypes.get(i).createBlockBuilder(null, 1);
        }
        return fieldBlockBuilders;
    }

    @UsedByGeneratedCode
    public static SqlRow createSqlRowFromFieldBuildersForSingleRow(BlockBuilder[] fieldBuilders)
    {
        Block[] fieldBlocks = new Block[fieldBuilders.length];
        for (int i = 0; i < fieldBuilders.length; i++) {
            fieldBlocks[i] = fieldBuilders[i].build();
            if (fieldBlocks[i].getPositionCount() != 1) {
                throw new IllegalArgumentException(format("builder must only contain a single position, found: %s positions", fieldBlocks[i].getPositionCount()));
            }
        }
        return new SqlRow(0, fieldBlocks);
    }
}
