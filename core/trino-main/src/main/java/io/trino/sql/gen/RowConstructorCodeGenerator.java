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
import io.trino.sql.gen.ExpressionBytecodeCompiler.LetBinding;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import static io.trino.sql.gen.BytecodeUtils.fitsJitCompilableMethod;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.util.CompilerUtils.isClassDumpEnabled;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowConstructorCodeGenerator
        implements BytecodeGenerator
{
    private final Row row;
    private final RowType rowType;
    private final List<Expression> arguments;
    // Arbitrary value chosen to balance the code size vs performance trade off. Not perf tested.
    static final int MEGAMORPHIC_FIELD_COUNT = 64;

    public RowConstructorCodeGenerator(Row row)
    {
        this.row = requireNonNull(row, "row is null");
        rowType = (RowType) row.type();
        arguments = row.items();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        if (arguments.size() > MEGAMORPHIC_FIELD_COUNT) {
            return generateExpressionForLargeRows(context);
        }

        BytecodeNode inline = generateInlineExpression(context);
        // a few bulky fields can exceed the JIT limit too
        if (fitsJitCompilableMethod(inline, context.getScope())) {
            return inline;
        }
        return generateExpressionForLargeRows(context);
    }

    private BytecodeNode generateInlineExpression(BytecodeGeneratorContext context)
    {
        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType);
        CallSiteBinder binder = context.getCallSiteBinder();
        Scope scope = context.getScope();
        List<Type> types = rowType.getFieldTypes();

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

            if (isClassDumpEnabled()) {
                block.comment("Clean wasNull and Generate + " + i + "-th field of row");
            }
            block.append(context.wasNull().set(constantFalse()));
            block.append(context.generate(arguments.get(i)));
            Variable field = scope.getOrCreateTempVariable(binder.getAccessibleType(fieldType.getJavaType()));
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

        List<LetBinding> lets = ImmutableList.copyOf(context.lets().values());
        // a failed inline attempt may already have declared these methods
        List<MethodDefinition> partials = context.getExpressionCompiler().partialRowConstructors(row, () -> generatePartialRowConstructors(context));
        for (MethodDefinition partial : partials) {
            block.getVariable(scope.getThis());
            for (Parameter argument : context.getContextArguments()) {
                block.getVariable(argument);
            }
            for (LetBinding let : lets) {
                block.getVariable(let.value());
                block.getVariable(let.wasNull());
            }
            block.getVariable(fieldBuilders);
            block.invokeVirtual(partial);
        }

        block.append(invokeStatic(RowConstructorCodeGenerator.class, "createSqlRowFromFieldBuildersForSingleRow", SqlRow.class, fieldBuilders));
        scope.releaseTempVariableForReuse(fieldBuilders);
        block.append(context.wasNull().set(constantFalse()));
        return block;
    }

    private List<MethodDefinition> generatePartialRowConstructors(BytecodeGeneratorContext context)
    {
        ImmutableList.Builder<MethodDefinition> methods = ImmutableList.builder();
        int field = 0;
        while (field < arguments.size()) {
            PartialRowConstructor partial = generatePartialRowConstructor(field, context);
            methods.add(partial.method());
            field = partial.nextField();
        }
        return methods.build();
    }

    private PartialRowConstructor generatePartialRowConstructor(int start, BytecodeGeneratorContext parentContext)
    {
        ClassDefinition classDefinition = parentContext.getClassDefinition();
        CallSiteBinder binder = parentContext.getCallSiteBinder();

        Parameter fieldBuilders = arg("fieldBuilders", BlockBuilder[].class);

        List<Parameter> parameters = new ArrayList<>(parentContext.getContextArguments());
        // let bindings are locals of the caller, so each one is passed in with its null flag
        List<LetBinding> parentLets = ImmutableList.copyOf(parentContext.lets().values());
        for (int i = 0; i < parentLets.size(); i++) {
            parameters.add(arg("let" + i, parentLets.get(i).value().getType()));
            parameters.add(arg("letWasNull" + i, boolean.class));
        }
        parameters.add(fieldBuilders);

        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PUBLIC),
                "partialRowConstructor" + System.identityHashCode(this) + "_" + start,
                type(void.class),
                parameters);

        Scope scope = methodDefinition.getScope();
        BytecodeBlock block = methodDefinition.getBody();
        scope.declareVariable("wasNull", block, constantFalse());

        Map<String, LetBinding> lets = new LinkedHashMap<>();
        int letIndex = 0;
        for (String name : parentContext.lets().keySet()) {
            lets.put(name, new LetBinding(scope.getVariable("let" + letIndex), scope.getVariable("letWasNull" + letIndex)));
            letIndex++;
        }

        BytecodeGeneratorContext context = new BytecodeGeneratorContext(
                parentContext.getExpressionCompiler(),
                scope,
                binder,
                parentContext.getCachedInstanceBinder(),
                parentContext.getFunctionManager(),
                parentContext.getMetadata(),
                classDefinition,
                parentContext.getContextArguments(),
                lets);
        Variable blockBuilder = scope.getOrCreateTempVariable(BlockBuilder.class);
        List<Type> types = rowType.getFieldTypes();

        // Pack fields greedily by estimated bytecode size instead of a fixed field count:
        // a fixed count of fields with bulky initialization (e.g. CASE over varchar or
        // LongTimestampWithTimeZone values) can exceed the JVM method size limit
        int field = start;
        while (field < arguments.size()) {
            Type fieldType = types.get(field);

            BytecodeBlock fieldInitialization = new BytecodeBlock();
            fieldInitialization.append(blockBuilder.set(fieldBuilders.getElement(constantInt(field))));

            if (isClassDumpEnabled()) {
                fieldInitialization.comment("Clean wasNull and Generate + " + field + "-th field of row");
            }

            fieldInitialization.append(context.wasNull().set(constantFalse()));
            fieldInitialization.append(context.generate(arguments.get(field)));
            Variable fieldVariable = scope.getOrCreateTempVariable(binder.getAccessibleType(fieldType.getJavaType()));
            fieldInitialization.putVariable(fieldVariable);
            fieldInitialization.append(new IfStatement()
                    .condition(context.wasNull())
                    .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, fieldType).writeValue(blockBuilder, fieldVariable).pop()));
            scope.releaseTempVariableForReuse(fieldVariable);

            BytecodeBlock candidate = new BytecodeBlock()
                    .append(block)
                    .append(fieldInitialization);
            // always emit at least one field, so an oversized single field still makes progress
            if (field > start && !fitsJitCompilableMethod(candidate, scope)) {
                // this field goes into the next partial method
                break;
            }
            block.append(fieldInitialization);
            field++;
        }

        block.ret();
        return new PartialRowConstructor(methodDefinition, field);
    }

    private record PartialRowConstructor(MethodDefinition method, int nextField) {}

    @UsedByGeneratedCode
    public static BlockBuilder[] createFieldBlockBuildersForSingleRow(Type type)
    {
        if (!(type instanceof RowType rowType)) {
            throw new IllegalArgumentException("Not a row type: " + type);
        }
        List<Type> fieldTypes = rowType.getFieldTypes();
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
