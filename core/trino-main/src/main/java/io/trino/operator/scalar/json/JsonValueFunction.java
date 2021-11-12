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
package io.trino.operator.scalar.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.JsonPath;
import io.trino.operator.scalar.ScalarFunctionImplementation;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.type.JsonPath2016Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.StandardTypes.JSON_2016;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class JsonValueFunction
        extends SqlScalarFunction
{
    public static final String JSON_VALUE_FUNCTION_NAME = "$json_value";
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonValueFunction.class, "jsonValue", Metadata.class, TypeManager.class, Type.class, Type.class, ConnectorSession.class, JsonNode.class, JsonPath.class, Object.class, long.class, Object.class, long.class, Object.class); // TODO replace JsonPath with IrJsonPath

    private final FunctionManager functionManager;
    private final Metadata metadata;
    private final TypeManager typeManager;

    public JsonValueFunction(FunctionManager functionManager, Metadata metadata, TypeManager typeManager)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(JSON_VALUE_FUNCTION_NAME)
                        .typeVariable("R")
                        .typeVariable("T")
                        .returnType(new TypeSignature("R"))
                        .argumentTypes(ImmutableList.of(
                                new TypeSignature(JSON_2016),
                                new TypeSignature(JsonPath2016Type.NAME),
                                new TypeSignature("T"),
                                new TypeSignature(TINYINT),
                                new TypeSignature("R"),
                                new TypeSignature(TINYINT),
                                new TypeSignature("R")))
                        .build())
                .nullable()
                .argumentNullability(false, false, true, false, true, false, true)
                .hidden()
                .description("Extracts an SQL scalar from a JSON value")
                .build());

        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        Type parametersRowType = boundSignature.getArgumentType(2);
        Type returnType = boundSignature.getArgumentType(4);
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(metadata)
                .bindTo(typeManager)
                .bindTo(parametersRowType)
                .bindTo(returnType);
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL, BOXED_NULLABLE, NEVER_NULL, BOXED_NULLABLE),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object jsonValue(
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            ConnectorSession session,
            JsonNode inputExpression,
            JsonPath jsonPath,
            Object parametersRow,
            long emptyBehavior,
            Object emptyDefault,
            long errorBehavior,
            Object errorDefault)
    {
        throw new UnsupportedOperationException("JSON_VALUE function is not yet supported");
    }
}
