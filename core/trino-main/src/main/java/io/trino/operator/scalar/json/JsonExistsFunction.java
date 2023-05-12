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
import io.trino.json.JsonPathEvaluator;
import io.trino.json.JsonPathInvocationContext;
import io.trino.json.PathEvaluationError;
import io.trino.json.ir.IrJsonPath;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.tree.JsonExists.ErrorBehavior;
import io.trino.type.JsonPath2016Type;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.json.JsonInputErrorNode.JSON_ERROR;
import static io.trino.operator.scalar.json.ParameterUtil.getParametersArray;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.StandardTypes.JSON_2016;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class JsonExistsFunction
        extends SqlScalarFunction
{
    public static final String JSON_EXISTS_FUNCTION_NAME = "$json_exists";
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonExistsFunction.class, "jsonExists", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, Block.class, long.class);

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
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type parametersRowType = boundSignature.getArgumentType(2);
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(functionManager)
                .bindTo(metadata)
                .bindTo(typeManager)
                .bindTo(parametersRowType);
        MethodHandle instanceFactory = constructorMethodHandle(JsonPathInvocationContext.class);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL),
                methodHandle,
                Optional.of(instanceFactory));
    }

    @UsedByGeneratedCode
    public static Boolean jsonExists(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Block parametersRow,
            long errorBehavior)
    {
        if (inputExpression.equals(JSON_ERROR)) {
            return handleError(errorBehavior, () -> new JsonInputConversionError("malformed input argument to JSON_EXISTS function")); // ERROR ON ERROR was already handled by the input function
        }
        Object[] parameters = getParametersArray(parametersRowType, parametersRow);
        for (Object parameter : parameters) {
            if (parameter.equals(JSON_ERROR)) {
                return handleError(errorBehavior, () -> new JsonInputConversionError("malformed JSON path parameter to JSON_EXISTS function")); // ERROR ON ERROR was already handled by the input function
            }
        }
        // The jsonPath argument is constant for every row. We use the first incoming jsonPath argument to initialize
        // the JsonPathEvaluator, and ignore the subsequent jsonPath values. We could sanity-check that all the incoming
        // jsonPath values are equal. We deliberately skip this costly check, since this is a hidden function.
        JsonPathEvaluator evaluator = invocationContext.getEvaluator();
        if (evaluator == null) {
            evaluator = new JsonPathEvaluator(jsonPath, session, metadata, typeManager, functionManager);
            invocationContext.setEvaluator(evaluator);
        }
        List<Object> pathResult;
        try {
            pathResult = evaluator.evaluate(inputExpression, parameters);
        }
        catch (PathEvaluationError e) {
            return handleError(errorBehavior, () -> e);
        }

        return !pathResult.isEmpty();
    }

    private static Boolean handleError(long errorBehavior, Supplier<TrinoException> error)
    {
        switch (ErrorBehavior.values()[(int) errorBehavior]) {
            case FALSE:
                return false;
            case TRUE:
                return true;
            case UNKNOWN:
                return null;
            case ERROR:
                throw error.get();
        }
        throw new IllegalStateException("unexpected error behavior");
    }
}
