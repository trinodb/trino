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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.StandardTypes.JSON_2016;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class JsonExistsFunction
        extends SqlScalarFunction
{
    public static final String JSON_EXISTS_FUNCTION_NAME = "$json_exists";
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonExistsFunction.class, "jsonExists", Metadata.class, TypeManager.class, Type.class, ConnectorSession.class, JsonNode.class, JsonPath.class, Object.class, long.class); // TODO replace JsonPath with IrJsonPath

    private final FunctionManager functionManager;
    private final Metadata metadata;
    private final TypeManager typeManager;

    public JsonExistsFunction(FunctionManager functionManager, Metadata metadata, TypeManager typeManager)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(JSON_EXISTS_FUNCTION_NAME)
                        .typeVariable("T")
                        .returnType(BOOLEAN)
                        .argumentTypes(ImmutableList.of(new TypeSignature(JSON_2016), new TypeSignature(JsonPath2016Type.NAME), new TypeSignature("T"), new TypeSignature(TINYINT)))
                        .build())
                .nullable()
                .argumentNullability(false, false, true, false)
                .hidden()
                .description("Determines whether a JSON value satisfies a path specification")
                .build());

        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        Type parametersRowType = boundSignature.getArgumentType(2);
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(metadata)
                .bindTo(typeManager)
                .bindTo(parametersRowType);
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Boolean jsonExists(
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            ConnectorSession session,
            JsonNode inputExpression,
            JsonPath jsonPath,
            Object parametersRow,
            long errorBehavior)
    {
        throw new UnsupportedOperationException("JSON_EXISTS function is not yet supported");
    }
}
