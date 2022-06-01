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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.tree.QualifiedName;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.Signature.isOperatorName;
import static io.trino.metadata.Signature.unmangleOperator;
import static io.trino.spi.function.OperatorType.CAST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class FunctionDependencies
{
    private final BiFunction<ResolvedFunction, InvocationConvention, FunctionInvoker> specialization;
    private final Map<TypeSignature, Type> types;
    private final Map<FunctionKey, ResolvedFunction> functions;
    private final Map<OperatorKey, ResolvedFunction> operators;
    private final Map<CastKey, ResolvedFunction> casts;

    public FunctionDependencies(
            BiFunction<ResolvedFunction, InvocationConvention, FunctionInvoker> specialization,
            Map<TypeSignature, Type> typeDependencies,
            Collection<ResolvedFunction> functionDependencies)
    {
        requireNonNull(specialization, "specialization is null");
        requireNonNull(typeDependencies, "typeDependencies is null");
        requireNonNull(functionDependencies, "functionDependencies is null");

        this.specialization = specialization;
        this.types = ImmutableMap.copyOf(typeDependencies);
        this.functions = functionDependencies.stream()
                .filter(function -> !isOperatorName(function.getSignature().getName()))
                .collect(toImmutableMap(FunctionKey::new, identity()));
        this.operators = functionDependencies.stream()
                .filter(FunctionDependencies::isOperator)
                .collect(toImmutableMap(OperatorKey::new, identity()));
        this.casts = functionDependencies.stream()
                .filter(FunctionDependencies::isCast)
                .collect(toImmutableMap(CastKey::new, identity()));
    }

    public Type getType(TypeSignature typeSignature)
    {
        // CHAR type does not properly roundtrip, so load directly from metadata and then verify type was declared correctly
        Type type = types.get(typeSignature);
        if (type == null) {
            throw new UndeclaredDependencyException(typeSignature.toString());
        }
        return type;
    }

    public FunctionNullability getFunctionNullability(QualifiedName name, List<Type> parameterTypes)
    {
        FunctionKey functionKey = new FunctionKey(name, toTypeSignatures(parameterTypes));
        ResolvedFunction resolvedFunction = functions.get(functionKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(functionKey.toString());
        }
        return resolvedFunction.getFunctionNullability();
    }

    public FunctionNullability getOperatorNullability(OperatorType operatorType, List<Type> parameterTypes)
    {
        OperatorKey operatorKey = new OperatorKey(operatorType, toTypeSignatures(parameterTypes));
        ResolvedFunction resolvedFunction = operators.get(operatorKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(operatorKey.toString());
        }
        return resolvedFunction.getFunctionNullability();
    }

    public FunctionNullability getCastNullability(Type fromType, Type toType)
    {
        CastKey castKey = new CastKey(fromType.getTypeSignature(), toType.getTypeSignature());
        ResolvedFunction resolvedFunction = casts.get(castKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(castKey.toString());
        }
        return resolvedFunction.getFunctionNullability();
    }

    public FunctionInvoker getFunctionInvoker(QualifiedName name, List<Type> parameterTypes, InvocationConvention invocationConvention)
    {
        FunctionKey functionKey = new FunctionKey(name, toTypeSignatures(parameterTypes));
        ResolvedFunction resolvedFunction = functions.get(functionKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(functionKey.toString());
        }
        return specialization.apply(resolvedFunction, invocationConvention);
    }

    public FunctionInvoker getFunctionSignatureInvoker(QualifiedName name, List<TypeSignature> parameterTypes, InvocationConvention invocationConvention)
    {
        FunctionKey functionKey = new FunctionKey(name, parameterTypes);
        ResolvedFunction resolvedFunction = functions.get(functionKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(functionKey.toString());
        }
        return specialization.apply(resolvedFunction, invocationConvention);
    }

    public FunctionInvoker getOperatorInvoker(OperatorType operatorType, List<Type> parameterTypes, InvocationConvention invocationConvention)
    {
        OperatorKey operatorKey = new OperatorKey(operatorType, toTypeSignatures(parameterTypes));
        ResolvedFunction resolvedFunction = operators.get(operatorKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(operatorKey.toString());
        }
        return specialization.apply(resolvedFunction, invocationConvention);
    }

    public FunctionInvoker getOperatorSignatureInvoker(OperatorType operatorType, List<TypeSignature> parameterTypes, InvocationConvention invocationConvention)
    {
        OperatorKey operatorKey = new OperatorKey(operatorType, parameterTypes);
        ResolvedFunction resolvedFunction = operators.get(operatorKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(operatorKey.toString());
        }
        return specialization.apply(resolvedFunction, invocationConvention);
    }

    public FunctionInvoker getCastInvoker(Type fromType, Type toType, InvocationConvention invocationConvention)
    {
        CastKey castKey = new CastKey(fromType.getTypeSignature(), toType.getTypeSignature());
        ResolvedFunction resolvedFunction = casts.get(castKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(castKey.toString());
        }
        return specialization.apply(resolvedFunction, invocationConvention);
    }

    public FunctionInvoker getCastSignatureInvoker(TypeSignature fromType, TypeSignature toType, InvocationConvention invocationConvention)
    {
        CastKey castKey = new CastKey(fromType, toType);
        ResolvedFunction resolvedFunction = casts.get(castKey);
        if (resolvedFunction == null) {
            throw new UndeclaredDependencyException(castKey.toString());
        }
        return specialization.apply(resolvedFunction, invocationConvention);
    }

    private static List<TypeSignature> toTypeSignatures(List<Type> types)
    {
        return types.stream()
                .map(Type::getTypeSignature)
                .collect(toImmutableList());
    }

    private static boolean isOperator(ResolvedFunction function)
    {
        String name = function.getSignature().getName();
        return isOperatorName(name) && unmangleOperator(name) != CAST;
    }

    private static boolean isCast(ResolvedFunction function)
    {
        String name = function.getSignature().getName();
        return isOperatorName(name) && unmangleOperator(name) == CAST;
    }

    public static final class FunctionKey
    {
        private final QualifiedName name;
        private final List<TypeSignature> argumentTypes;

        private FunctionKey(ResolvedFunction resolvedFunction)
        {
            Signature signature = resolvedFunction.getSignature().toSignature();
            name = QualifiedName.of(signature.getName());
            argumentTypes = resolvedFunction.getSignature().getArgumentTypes().stream()
                    .map(Type::getTypeSignature)
                    .collect(toImmutableList());
        }

        private FunctionKey(QualifiedName name, List<TypeSignature> argumentTypes)
        {
            this.name = requireNonNull(name, "name is null");
            this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionKey that = (FunctionKey) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(argumentTypes, that.argumentTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, argumentTypes);
        }

        @Override
        public String toString()
        {
            return name + argumentTypes.stream()
                    .map(TypeSignature::toString)
                    .collect(Collectors.joining(", ", "(", ")"));
        }
    }

    public static final class OperatorKey
    {
        private final OperatorType operatorType;
        private final List<TypeSignature> argumentTypes;

        private OperatorKey(ResolvedFunction resolvedFunction)
        {
            operatorType = unmangleOperator(resolvedFunction.getSignature().getName());
            argumentTypes = toTypeSignatures(resolvedFunction.getSignature().getArgumentTypes());
        }

        private OperatorKey(OperatorType operatorType, List<TypeSignature> argumentTypes)
        {
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
            this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OperatorKey that = (OperatorKey) o;
            return operatorType == that.operatorType &&
                    Objects.equals(argumentTypes, that.argumentTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operatorType, argumentTypes);
        }

        @Override
        public String toString()
        {
            return operatorType + argumentTypes.stream()
                    .map(TypeSignature::toString)
                    .collect(Collectors.joining(", ", "(", ")"));
        }
    }

    private static final class CastKey
    {
        private final TypeSignature fromType;
        private final TypeSignature toType;

        private CastKey(ResolvedFunction resolvedFunction)
        {
            fromType = resolvedFunction.getSignature().getArgumentTypes().get(0).getTypeSignature();
            toType = resolvedFunction.getSignature().getReturnType().getTypeSignature();
        }

        private CastKey(TypeSignature fromType, TypeSignature toType)
        {
            this.fromType = fromType;
            this.toType = toType;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CastKey that = (CastKey) o;
            return Objects.equals(fromType, that.fromType) &&
                    Objects.equals(toType, that.toType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fromType, toType);
        }

        @Override
        public String toString()
        {
            return format("cast(%s, %s)", fromType, toType);
        }
    }
}
