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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionDependencies;
import io.prestosql.metadata.FunctionDependencyDeclaration;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DuplicateMapKeyException;
import io.prestosql.spi.block.MapBlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.TypeSignature.arrayType;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Failures.internalError;
import static io.prestosql.util.Reflection.constructorMethodHandle;
import static io.prestosql.util.Reflection.methodHandle;

public final class MapConstructor
        extends SqlScalarFunction
{
    public static final MapConstructor MAP_CONSTRUCTOR = new MapConstructor();

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapConstructor.class,
            "createMap",
            MapType.class,
            MethodHandle.class,
            MethodHandle.class,
            MethodHandle.class,
            State.class,
            ConnectorSession.class,
            Block.class,
            Block.class);

    private static final String DESCRIPTION = "Constructs a map from the given key/value arrays";

    public MapConstructor()
    {
        super(new FunctionMetadata(
                new Signature(
                        "map",
                        ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                        ImmutableList.of(),
                        TypeSignature.mapType(new TypeSignature("K"), new TypeSignature("V")),
                        ImmutableList.of(arrayType(new TypeSignature("K")), arrayType(new TypeSignature("V"))),
                        false),
                false,
                ImmutableList.of(
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false)),
                false,
                true,
                DESCRIPTION,
                SCALAR));
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(HASH_CODE, ImmutableList.of(new TypeSignature("K")))
                .addOperatorSignature(EQUAL, ImmutableList.of(new TypeSignature("K"), new TypeSignature("K")))
                .addOperatorSignature(INDETERMINATE, ImmutableList.of(new TypeSignature("K")))
                .build();
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Type keyType = functionBinding.getTypeVariable("K");

        MethodHandle keyHashCode = functionDependencies.getOperatorInvoker(HASH_CODE, ImmutableList.of(keyType), Optional.empty()).getMethodHandle();
        MethodHandle keyEqual = functionDependencies.getOperatorInvoker(EQUAL, ImmutableList.of(keyType, keyType), Optional.empty()).getMethodHandle();
        InvocationConvention indeterminateCallingConvention = new InvocationConvention(ImmutableList.of(NULL_FLAG), FAIL_ON_NULL, false, false);
        MethodHandle keyIndeterminate = functionDependencies.getOperatorInvoker(INDETERMINATE, ImmutableList.of(keyType), Optional.of(indeterminateCallingConvention)).getMethodHandle();

        Type mapType = functionBinding.getBoundSignature().getReturnType();
        MethodHandle instanceFactory = constructorMethodHandle(State.class, MapType.class).bindTo(mapType);

        return new ScalarFunctionImplementation(
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE.bindTo(mapType).bindTo(keyEqual).bindTo(keyHashCode).bindTo(keyIndeterminate),
                Optional.of(instanceFactory));
    }

    @UsedByGeneratedCode
    public static Block createMap(
            MapType mapType,
            MethodHandle keyEqual,
            MethodHandle keyHashCode,
            MethodHandle keyIndeterminate,
            State state,
            ConnectorSession session,
            Block keyBlock,
            Block valueBlock)
    {
        checkCondition(keyBlock.getPositionCount() == valueBlock.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "Key and value arrays must be the same length");
        PageBuilder pageBuilder = state.getPageBuilder();
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < keyBlock.getPositionCount(); i++) {
            if (keyBlock.isNull(i)) {
                // close block builder before throwing as we may be in a TRY() call
                // so that subsequent calls do not find it in an inconsistent state
                mapBlockBuilder.closeEntry();
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }
            Object keyObject = readNativeValue(mapType.getKeyType(), keyBlock, i);
            try {
                if ((boolean) keyIndeterminate.invoke(keyObject, false)) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be indeterminate: " + mapType.getKeyType().getObjectValue(session, keyBlock, i));
                }
            }
            catch (Throwable t) {
                mapBlockBuilder.closeEntry();
                throw internalError(t);
            }
            mapType.getKeyType().appendTo(keyBlock, i, blockBuilder);
            mapType.getValueType().appendTo(valueBlock, i, blockBuilder);
        }
        try {
            mapBlockBuilder.closeEntryStrict();
        }
        catch (DuplicateMapKeyException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getDetailedMessage(mapType.getKeyType(), session), e);
        }
        finally {
            pageBuilder.declarePosition();
        }

        return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }

    public static final class State
    {
        private final PageBuilder pageBuilder;

        public State(MapType mapType)
        {
            pageBuilder = new PageBuilder(ImmutableList.of(mapType));
        }

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }
    }
}
