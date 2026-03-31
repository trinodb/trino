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
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.json.JsonPathEvaluator;
import io.trino.json.JsonPathInvocationContext;
import io.trino.json.PathEvaluationException;
import io.trino.json.ir.IrJsonPath;
import io.trino.json.ir.JsonLiteralConversionException;
import io.trino.json.ir.TypedValue;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;
import io.trino.sql.tree.JsonValue.EmptyOrErrorBehavior;
import io.trino.type.FunctionType;
import io.trino.type.JsonPath2016Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.json.JsonInputErrorNode.JSON_ERROR;
import static io.trino.json.ir.SqlJsonLiteralConverter.getTypedValue;
import static io.trino.operator.scalar.json.ParameterUtil.getParametersArray;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.StandardTypes.JSON_2016;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.util.Objects.requireNonNull;

public class JsonValueFunction
        extends SqlScalarFunction
{
    public static final String JSON_VALUE_FUNCTION_NAME = "$json_value";
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(JsonValueFunction.class, "jsonValueLong", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, MethodHandle.class, MethodHandle.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, SqlRow.class, long.class, DefaultValueLambda.class, long.class, DefaultValueLambda.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(JsonValueFunction.class, "jsonValueDouble", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, MethodHandle.class, MethodHandle.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, SqlRow.class, long.class, DefaultValueLambda.class, long.class, DefaultValueLambda.class);
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(JsonValueFunction.class, "jsonValueBoolean", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, MethodHandle.class, MethodHandle.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, SqlRow.class, long.class, DefaultValueLambda.class, long.class, DefaultValueLambda.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(JsonValueFunction.class, "jsonValueSlice", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, MethodHandle.class, MethodHandle.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, SqlRow.class, long.class, DefaultValueLambda.class, long.class, DefaultValueLambda.class);
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonValueFunction.class, "jsonValue", FunctionManager.class, Metadata.class, TypeManager.class, Type.class, Type.class, MethodHandle.class, MethodHandle.class, JsonPathInvocationContext.class, ConnectorSession.class, JsonNode.class, IrJsonPath.class, SqlRow.class, long.class, DefaultValueLambda.class, long.class, DefaultValueLambda.class);

    private final FunctionManager functionManager;
    private final Metadata metadata;
    private final TypeManager typeManager;

    public JsonValueFunction(FunctionManager functionManager, Metadata metadata, TypeManager typeManager)
    {
        super(FunctionMetadata.scalarBuilder(JSON_VALUE_FUNCTION_NAME)
                .signature(Signature.builder()
                        .typeVariable("R")
                        .typeVariable("T")
                        .typeVariable("E")
                        .typeVariable("D")
                        .returnType(new TypeSignature("R"))
                        .argumentTypes(ImmutableList.of(
                                new TypeSignature(JSON_2016),
                                new TypeSignature(JsonPath2016Type.NAME),
                                new TypeSignature("T"),
                                new TypeSignature("R"),
                                new TypeSignature(TINYINT),
                                functionType(new TypeSignature("E")),
                                new TypeSignature(TINYINT),
                                functionType(new TypeSignature("D"))))
                        .build())
                .nullable()
                .argumentNullability(false, false, true, true, false, false, false, false)
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
        Type emptyDefaultType = ((FunctionType) boundSignature.getArgumentType(5)).getReturnType();
        Type errorDefaultType = ((FunctionType) boundSignature.getArgumentType(7)).getReturnType();
        MethodHandle emptyDefaultCoercion = createDefaultCoercion(functionManager, metadata, emptyDefaultType, returnType);
        MethodHandle errorDefaultCoercion = createDefaultCoercion(functionManager, metadata, errorDefaultType, returnType);

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
                .bindTo(returnType)
                .bindTo(emptyDefaultCoercion)
                .bindTo(errorDefaultCoercion);
        methodHandle = dropArguments(methodHandle, 5, Primitives.wrap(boundSignature.getArgumentType(3).getJavaType()));
        MethodHandle instanceFactory = constructorMethodHandle(JsonPathInvocationContext.class);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL, FUNCTION, NEVER_NULL, FUNCTION),
                ImmutableList.of(DefaultValueLambda.class, DefaultValueLambda.class),
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
            MethodHandle emptyDefaultCoercion,
            MethodHandle errorDefaultCoercion,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            SqlRow parametersRow,
            long emptyBehavior,
            DefaultValueLambda emptyDefault,
            long errorBehavior,
            DefaultValueLambda errorDefault)
    {
        return (Long) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, emptyDefaultCoercion, errorDefaultCoercion, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Double jsonValueDouble(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            MethodHandle emptyDefaultCoercion,
            MethodHandle errorDefaultCoercion,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            SqlRow parametersRow,
            long emptyBehavior,
            DefaultValueLambda emptyDefault,
            long errorBehavior,
            DefaultValueLambda errorDefault)
    {
        return (Double) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, emptyDefaultCoercion, errorDefaultCoercion, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Boolean jsonValueBoolean(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            MethodHandle emptyDefaultCoercion,
            MethodHandle errorDefaultCoercion,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            SqlRow parametersRow,
            long emptyBehavior,
            DefaultValueLambda emptyDefault,
            long errorBehavior,
            DefaultValueLambda errorDefault)
    {
        return (Boolean) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, emptyDefaultCoercion, errorDefaultCoercion, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Slice jsonValueSlice(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            MethodHandle emptyDefaultCoercion,
            MethodHandle errorDefaultCoercion,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            SqlRow parametersRow,
            long emptyBehavior,
            DefaultValueLambda emptyDefault,
            long errorBehavior,
            DefaultValueLambda errorDefault)
    {
        return (Slice) jsonValue(functionManager, metadata, typeManager, parametersRowType, returnType, emptyDefaultCoercion, errorDefaultCoercion, invocationContext, session, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Object jsonValue(
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            Type parametersRowType,
            Type returnType,
            MethodHandle emptyDefaultCoercion,
            MethodHandle errorDefaultCoercion,
            JsonPathInvocationContext invocationContext,
            ConnectorSession session,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            SqlRow parametersRow,
            long emptyBehavior,
            DefaultValueLambda emptyDefault,
            long errorBehavior,
            DefaultValueLambda errorDefault)
    {
        if (inputExpression.equals(JSON_ERROR)) {
            return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonInputConversionException("malformed input argument to JSON_VALUE function")); // ERROR ON ERROR was already handled by the input function
        }
        Object[] parameters = getParametersArray(parametersRowType, parametersRow);
        for (Object parameter : parameters) {
            if (parameter.equals(JSON_ERROR)) {
                return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonInputConversionException("malformed JSON path parameter to JSON_VALUE function")); // ERROR ON ERROR was already handled by the input function
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
        catch (PathEvaluationException e) {
            return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> e);
        }

        if (pathResult.isEmpty()) {
            return handleEmpty(session, emptyBehavior, emptyDefaultCoercion, emptyDefault, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonValueResultException("JSON path found no items"));
        }

        if (pathResult.size() > 1) {
            return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonValueResultException("JSON path found multiple items"));
        }
        Object item = getOnlyElement(pathResult);
        TypedValue typedValue;
        if (item instanceof JsonNode jsonNode) {
            if (item.equals(NullNode.instance)) {
                return null;
            }
            Optional<TypedValue> itemValue;
            try {
                itemValue = getTypedValue(jsonNode);
            }
            catch (JsonLiteralConversionException e) {
                return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonValueResultException("JSON path found an item that cannot be converted to an SQL value", e));
            }
            if (itemValue.isEmpty()) {
                return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonValueResultException("JSON path found an item that cannot be converted to an SQL value"));
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
            coercion = metadata.getCoercion(typedValue.getType(), returnType);
        }
        catch (OperatorNotFoundException e) {
            return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonValueResultException(format(
                    "Cannot cast value of type %s to declared return type of function JSON_VALUE: %s",
                    typedValue.getType(),
                    returnType)));
        }
        try {
            return new InterpretedFunctionInvoker(functionManager).invoke(coercion, session, ImmutableList.of(typedValue.getValueAsObject()));
        }
        catch (RuntimeException e) {
            return handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> new JsonValueResultException(format(
                    "Cannot cast value of type %s to declared return type of function JSON_VALUE: %s",
                    typedValue.getType(),
                    returnType)));
        }
    }

    private static Object handleEmpty(
            ConnectorSession session,
            long emptyBehavior,
            MethodHandle emptyDefaultCoercion,
            DefaultValueLambda emptyDefault,
            long errorBehavior,
            MethodHandle errorDefaultCoercion,
            DefaultValueLambda errorDefault,
            Supplier<RuntimeException> error)
    {
        return switch (EmptyOrErrorBehavior.values()[(int) emptyBehavior]) {
            case NULL -> null;
            case ERROR -> throw error.get();
            case DEFAULT -> {
                try {
                    yield evaluateDefault(session, emptyDefaultCoercion, emptyDefault);
                }
                catch (TrinoException defaultError) {
                    // Only handle evaluation errors (e.g., division by zero, cast overflow) by cascading
                    // to the ON ERROR clause. Other errors indicate unexpected failures and should propagate.
                    TryFunction.propagateIfUnhandled(defaultError);
                    yield handleError(session, errorBehavior, errorDefaultCoercion, errorDefault, () -> defaultError);
                }
            }
        };
    }

    private static Object handleError(
            ConnectorSession session,
            long behavior,
            MethodHandle defaultCoercion,
            DefaultValueLambda defaultValue,
            Supplier<RuntimeException> error)
    {
        return switch (EmptyOrErrorBehavior.values()[(int) behavior]) {
            case NULL -> null;
            case ERROR -> throw error.get();
            case DEFAULT -> evaluateDefault(session, defaultCoercion, defaultValue);
        };
    }

    private static Object evaluateDefault(ConnectorSession session, MethodHandle defaultCoercion, DefaultValueLambda defaultValue)
    {
        Object defaultResult = defaultValue.apply();
        if (defaultResult == null) {
            return null;
        }
        try {
            return defaultCoercion.invoke(session, defaultResult);
        }
        catch (Throwable t) {
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    private static MethodHandle createDefaultCoercion(FunctionManager functionManager, Metadata metadata, Type defaultType, Type returnType)
    {
        if (defaultType.equals(returnType)) {
            // identity: (ConnectorSession, Object) -> Object, returns the value as-is
            return dropArguments(MethodHandles.identity(Object.class), 0, ConnectorSession.class);
        }
        ResolvedFunction coercion = metadata.getCoercion(defaultType, returnType);
        MethodHandle coercionHandle = functionManager.getScalarFunctionImplementation(
                coercion,
                new InvocationConvention(ImmutableList.of(BOXED_NULLABLE), NULLABLE_RETURN, true, false))
                .getMethodHandle();
        if (!coercionHandle.type().parameterType(0).equals(ConnectorSession.class)) {
            coercionHandle = dropArguments(coercionHandle, 0, ConnectorSession.class);
        }
        return coercionHandle.asType(MethodType.methodType(Object.class, ConnectorSession.class, Object.class));
    }

    @FunctionalInterface
    public interface DefaultValueLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }
}
