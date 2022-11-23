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
package io.trino.operator.scalar;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.CachedInstanceBinder;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.explicitCastArguments;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.zero;
import static java.lang.invoke.MethodType.methodType;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RowToRowCast
        extends SqlScalarFunction
{
    private static final MethodHandle OBJECT_IS_NULL;
    private static final MethodHandle BLOCK_IS_NULL;
    private static final MethodHandle WRITE_BOOLEAN;
    private static final MethodHandle WRITE_LONG;
    private static final MethodHandle WRITE_DOUBLE;
    private static final MethodHandle WRITE_OBJECT;
    private static final MethodHandle APPEND_NULL;

    static {
        try {
            OBJECT_IS_NULL = lookup().findStatic(Objects.class, "isNull", methodType(boolean.class, Object.class));
            BLOCK_IS_NULL = lookup().findVirtual(Block.class, "isNull", methodType(boolean.class, int.class));

            WRITE_BOOLEAN = lookup().findVirtual(Type.class, "writeBoolean", methodType(void.class, BlockBuilder.class, boolean.class));
            WRITE_LONG = lookup().findVirtual(Type.class, "writeLong", methodType(void.class, BlockBuilder.class, long.class));
            WRITE_DOUBLE = lookup().findVirtual(Type.class, "writeDouble", methodType(void.class, BlockBuilder.class, double.class));
            WRITE_OBJECT = lookup().findVirtual(Type.class, "writeObject", methodType(void.class, BlockBuilder.class, Object.class));
            APPEND_NULL = lookup().findVirtual(BlockBuilder.class, "appendNull", methodType(BlockBuilder.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public static final RowToRowCast ROW_TO_ROW_CAST = new RowToRowCast();

    private RowToRowCast()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(CAST)
                        .typeVariableConstraint(
                                // this is technically a recursive constraint for cast, but TypeRegistry.canCast has explicit handling for row to row cast
                                TypeVariableConstraint.builder("F")
                                        .variadicBound("row")
                                        .castableTo(new TypeSignature("T"))
                                        .build())
                        .variadicTypeParameter("T", "row")
                        .returnType(new TypeSignature("T"))
                        .argumentType(new TypeSignature("F"))
                        .build())
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature)
    {
        List<Type> toTypes = boundSignature.getReturnType().getTypeParameters();
        List<Type> fromTypes = boundSignature.getArgumentType(0).getTypeParameters();

        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        for (int i = 0; i < toTypes.size(); i++) {
            Type fromElementType = fromTypes.get(i);
            Type toElementType = toTypes.get(i);
            builder.addCast(fromElementType, toElementType);
        }
        return builder.build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        Type fromType = boundSignature.getArgumentType(0);
        Type toType = boundSignature.getReturnType();
        if (fromType.getTypeParameters().size() != toType.getTypeParameters().size()) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "the size of fromType and toType must match");
        }
        Class<?> castOperatorClass = generateRowCast(fromType, toType, functionDependencies);
        MethodHandle methodHandle = methodHandle(castOperatorClass, "castRow", ConnectorSession.class, Block.class);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    private static Class<?> generateRowCast(Type fromType, Type toType, FunctionDependencies functionDependencies)
    {
        List<Type> toTypes = toType.getTypeParameters();
        List<Type> fromTypes = fromType.getTypeParameters();

        CallSiteBinder binder = new CallSiteBinder();

        // Embed the hash code of input and output types into the generated class name instead of the raw type names,
        // which ensures the class name does not hit the length limitation or invalid characters.
        byte[] hashSuffix = Hashing.goodFastHash(128).hashBytes((fromType + "$" + toType).getBytes(UTF_8)).asBytes();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Joiner.on("$").join("RowCast", BaseEncoding.base16().encode(hashSuffix))),
                type(Object.class));

        Parameter session = arg("session", ConnectorSession.class);
        Parameter row = arg("row", Block.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "castRow",
                type(Block.class),
                session,
                row);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        Variable blockBuilder = scope.createTempVariable(BlockBuilder.class);
        Variable singleRowBlockWriter = scope.createTempVariable(BlockBuilder.class);

        body.append(wasNull.set(constantBoolean(false)));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, binder);

        // create the row block builder
        body.append(blockBuilder.set(
                constantType(binder, toType).invoke(
                        "createBlockBuilder",
                        BlockBuilder.class,
                        constantNull(BlockBuilderStatus.class),
                        constantInt(1))));
        body.append(singleRowBlockWriter.set(blockBuilder.invoke("beginBlockEntry", BlockBuilder.class)));

        // loop through to append member blocks
        for (int i = 0; i < toTypes.size(); i++) {
            Type fromElementType = fromTypes.get(i);
            Type toElementType = toTypes.get(i);

            Type currentFromType = fromElementType;
            if (currentFromType.equals(UNKNOWN)) {
                body.append(singleRowBlockWriter.invoke("appendNull", BlockBuilder.class).pop());
                continue;
            }

            MethodHandle castMethod = getNullSafeCast(functionDependencies, fromElementType, toElementType);
            MethodHandle writeMethod = getNullSafeWrite(toElementType);
            MethodHandle castAndWrite = collectArguments(writeMethod, 1, castMethod);
            body.append(invokeDynamic(
                    BOOTSTRAP_METHOD,
                    ImmutableList.of(binder.bind(castAndWrite).getBindingId()),
                    "castAndWriteField",
                    castAndWrite.type(),
                    singleRowBlockWriter,
                    scope.getVariable("session"),
                    row,
                    constantInt(i)));
        }

        // call blockBuilder.closeEntry() and return the single row block
        body.append(blockBuilder.invoke("closeEntry", BlockBuilder.class).pop());
        body.append(constantType(binder, toType)
                .invoke("getObject", Object.class, blockBuilder.cast(Block.class), constantInt(0))
                .cast(Block.class)
                .ret());

        // create constructor
        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(definition, Object.class, binder.getBindings(), RowToRowCast.class.getClassLoader());
    }

    private static MethodHandle getNullSafeWrite(Type type)
    {
        MethodHandle writeMethod;
        if (type.getJavaType() == boolean.class) {
            writeMethod = WRITE_BOOLEAN;
        }
        else if (type.getJavaType() == long.class) {
            writeMethod = WRITE_LONG;
        }
        else if (type.getJavaType() == double.class) {
            writeMethod = WRITE_DOUBLE;
        }
        else {
            writeMethod = WRITE_OBJECT;
        }
        writeMethod = writeMethod.bindTo(type);
        writeMethod = explicitCastArguments(writeMethod, methodType(void.class, BlockBuilder.class, Object.class));
        MethodHandle isNull = dropArguments(OBJECT_IS_NULL, 0, BlockBuilder.class);
        MethodHandle appendNull = dropArguments(APPEND_NULL, 1, Object.class).asType(writeMethod.type());
        return guardWithTest(isNull, appendNull, writeMethod);
    }

    private static MethodHandle getNullSafeCast(FunctionDependencies functionDependencies, Type fromElementType, Type toElementType)
    {
        MethodHandle castMethod = functionDependencies.getCastImplementation(
                fromElementType,
                toElementType,
                new InvocationConvention(ImmutableList.of(BLOCK_POSITION), NULLABLE_RETURN, true, false))
                .getMethodHandle();

        // normalize so cast always has a session
        if (!castMethod.type().parameterType(0).equals(ConnectorSession.class)) {
            castMethod = dropArguments(castMethod, 0, ConnectorSession.class);
        }

        // change return to Object
        castMethod = castMethod.asType(methodType(Object.class, ConnectorSession.class, Block.class, int.class));

        // if block is null, return null. otherwise execute the cast
        return guardWithTest(
                dropArguments(BLOCK_IS_NULL, 0, ConnectorSession.class),
                dropArguments(zero(Object.class), 0, castMethod.type().parameterList()),
                castMethod);
    }
}
