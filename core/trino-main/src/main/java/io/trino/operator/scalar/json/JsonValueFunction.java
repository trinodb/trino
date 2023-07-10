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
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.json.JsonPathEvaluator;
import io.trino.json.JsonPathInvocationContext;
import io.trino.json.PathEvaluationError;
import io.trino.json.ir.IrJsonPath;
import io.trino.json.ir.SqlJsonLiteralConverter.JsonLiteralConversionError;
import io.trino.json.ir.TypedValue;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
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
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.tree.JsonValue.EmptyOrErrorBehavior;
import io.trino.type.JsonPath2016Type;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.json.JsonInputErrorNode.JSON_ERROR;
import static io.trino.json.ir.SqlJsonLiteralConverter.getTypedValue;
import static io.trino.operator.scalar.json.ParameterUtil.getParametersArray;
import static io.trino.spi.StandardErrorCode.JSON_VALUE_RESULT_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.StandardTypes.JSON_2016;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonValueFunction
        extends SqlScalarFunction
{
    public static final String JSON_VALUE_FUNCTION_NAME = "$json_value";
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(JsonValueFunction.class, "jsonValueLong", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, Block.class, long.class, Long.class, long.class, Long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(JsonValueFunction.class, "jsonValueDouble", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, Block.class, long.class, Double.class, long.class, Double.class);
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(JsonValueFunction.class, "jsonValueBoolean", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, Block.class, long.class, Boolean.class, long.class, Boolean.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(JsonValueFunction.class, "jsonValueSlice", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, Block.class, long.class, Slice.class, long.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonValueFunction.class, "jsonValue", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, Block.class, long.class, Object.class, long.class, Object.class);

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
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type parametersRowType = boundSignature.getArgumentType(2);
        Type returnType = boundSignature.getReturnType();
        MethodHandle handle;
        if (returnType.getJavaType().equals(long.class)) {
            handle = METHOD_HANDLE_LONG;
        }
        else if (returnType.getJavaType().equals(double.class)) {
            handle = METHOD_HANDLE_DOUBLE;
        }
        else if (returnType.getJavaType().equals(boolean.class)) {
            handle = METHOD_HANDLE_BOOLEAN;
        }
        else if (returnType.getJavaType().equals(Slice.class)) {
            handle = METHOD_HANDLE_SLICE;
        }
        else {
            handle = METHOD_HANDLE;
        }

        MethodHandle methodHandle = handle
                .bindTo(functionManager)
                .bindTo(metadata)
                .bindTo(typeManager)
                .bindTo(parametersRowType)
                .bindTo(returnType);
        MethodHandle instanceFactory = constructorMethodHandle(JsonPathInvocationContext.class);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL, BOXED_NULLABLE, NEVER_NULL, BOXED_NULLABLE),
                methodHandle,
                Optional.of(instanceFactory));
    }

    @UsedByGeneratedCode
    public static Long jsonValueLong(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Block parametersRow,
            long emptyBehavior,
            Long emptyDefault,
            long errorBehavior,
            Long errorDefault)
    {
        return (Long) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Double jsonValueDouble(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Block parametersRow,
            long emptyBehavior,
            Double emptyDefault,
            long errorBehavior,
            Double errorDefault)
    {
        return (Double) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Boolean jsonValueBoolean(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Block parametersRow,
            long emptyBehavior,
            Boolean emptyDefault,
            long errorBehavior,
            Boolean errorDefault)
    {
        return (Boolean) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Slice jsonValueSlice(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Block parametersRow,
            long emptyBehavior,
            Slice emptyDefault,
            long errorBehavior,
            Slice errorDefault)
    {
        return (Slice) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Object jsonValue(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Block parametersRow,
            long emptyBehavior,
            Object emptyDefault,
            long errorBehavior,
            Object errorDefault)
    {
        if (inputExpression.equals(JSON_ERROR)) {
            return handleSpecialCase(errorBehavior, errorDefault, () -> new JsonInputConversionError("malformed input argument to JSON_VALUE function")); // ERROR ON ERROR was already handled by the input function
        }
        Object[] parameters = getParametersArray(parametersRowType, parametersRow);
        for (Object parameter : parameters) {
            if (parameter.equals(JSON_ERROR)) {
                return handleSpecialCase(errorBehavior, errorDefault, () -> new JsonInputConversionError("malformed JSON path parameter to JSON_VALUE function")); // ERROR ON ERROR was already handled by the input function
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
            return handleSpecialCase(errorBehavior, errorDefault, () -> e); // TODO by spec, we should cast the defaults only if they are used
        }

        if (pathResult.isEmpty()) {
            return handleSpecialCase(emptyBehavior, emptyDefault, () -> new JsonValueResultError("JSON path found no items"));
        }

        if (pathResult.size() > 1) {
            return handleSpecialCase(errorBehavior, errorDefault, () -> new JsonValueResultError("JSON path found multiple items"));
        }

        Object item = getOnlyElement(pathResult);
        TypedValue typedValue;
        if (item instanceof JsonNode) {
            if (item.equals(NullNode.instance)) {
                return null;
            }
            Optional<TypedValue> itemValue;
            try {
                itemValue = getTypedValue((JsonNode) item);
            }
            catch (JsonLiteralConversionError e) {
                return handleSpecialCase(errorBehavior, errorDefault, () -> new JsonValueResultError("JSON path found an item that cannot be converted to an SQL value", e));
            }
            if (itemValue.isEmpty()) {
                return handleSpecialCase(errorBehavior, errorDefault, () -> new JsonValueResultError("JSON path found an item that cannot be converted to an SQL value"));
            }
            typedValue = itemValue.get();
        }
        else {
            typedValue = (TypedValue) item;
        }
        if (returnType.equals(typedValue.getType())) {
            return typedValue.getValueAsObject();
        }
        ResolvedFunction coercion;
        try {
            coercion = metadata.getCoercion(((FullConnectorSession) session).getSession(), typedValue.getType(), returnType);
        }
        catch (OperatorNotFoundException e) {
            return handleSpecialCase(errorBehavior, errorDefault, () -> new JsonValueResultError(format(
                    "Cannot cast value of type %s to declared return type of function JSON_VALUE: %s",
                    typedValue.getType(),
                    returnType)));
        }
        try {
            return new InterpretedFunctionInvoker(functionManager).invoke(coercion, session, ImmutableList.of(typedValue.getValueAsObject()));
        }
        catch (RuntimeException e) {
            return handleSpecialCase(errorBehavior, errorDefault, () -> new JsonValueResultError(format(
                    "Cannot cast value of type %s to declared return type of function JSON_VALUE: %s",
                    typedValue.getType(),
                    returnType)));
        }
    }

    private static Object handleSpecialCase(long behavior, Object defaultValue, Supplier<TrinoException> error)
    {
        switch (EmptyOrErrorBehavior.values()[(int) behavior]) {
            case NULL:
                return null;
            case ERROR:
                throw error.get();
            case DEFAULT:
                return defaultValue;
        }
        throw new IllegalStateException("unexpected behavior");
    }

    public static class JsonValueResultError
            extends TrinoException
    {
        public JsonValueResultError(String message)
        {
            super(JSON_VALUE_RESULT_ERROR, "cannot extract SQL scalar from JSON: " + message);
        }

        public JsonValueResultError(String message, Throwable cause)
        {
            super(JSON_VALUE_RESULT_ERROR, "cannot extract SQL scalar from JSON: " + message, cause);
        }
    }
}
