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

import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.DomainProjection;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.PreimageFunctionDependencies;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.function.PreimageResult.Exactness;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.type.CharVarcharCoercion;
import io.trino.type.TypeCoercion;

import java.lang.invoke.MethodHandle;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

/// Finds input values for which a scalar function produces a result in a requested set.
/// For example, the preimage of the year 2025 through `year(date)` is the set of dates in 2025.
/// The function's registered provider supplies this knowledge. This class supplies the
/// session, concrete argument and result types, fixed argument values, and operations
/// the provider can use to evaluate the function or convert boundary values.
///
/// An instance resolves each projection, function implementation, cast, and comparator at most
/// once and reuses it for every call with the same shape. Requests supply the session, so one
/// instance can serve a whole unit of work, such as an optimizer pass or a dynamic filter
/// translation. Instances are thread safe.
public final class FunctionPreimages
{
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final TypeManager typeManager;

    private final Map<ResolvedFunction, Optional<DomainProjection>> projections = new ConcurrentHashMap<>();
    private final Map<ResolvedFunction, Invoker> invokers = new ConcurrentHashMap<>();
    private final Map<CoercionKey, Optional<ResolvedFunction>> coercions = new ConcurrentHashMap<>();
    private final Map<Type, Comparator<Object>> comparators = new ConcurrentHashMap<>();
    private final Map<CharVarcharCoercion, TypeCoercion> typeCoercions = new ConcurrentHashMap<>();

    public FunctionPreimages(Metadata metadata, FunctionManager functionManager, TypeManager typeManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    /// Pairs a registered provider with the information for one function call. The returned
    /// object reuses that information for requests about different result sets or constants.
    /// An empty result means that the function is nondeterministic or has no registered provider.
    public Optional<BoundPreimage> bind(Session session, ResolvedFunction function, int argument, List<Optional<NullableValue>> constants, Exactness exactness)
    {
        // Function-typed arguments cannot be constants, so such a call has no projectable shape.
        if (!function.deterministic() || function.signature().getArgumentTypes().stream().anyMatch(FunctionType.class::isInstance)) {
            return Optional.empty();
        }
        return projection(session, function).map(projection -> {
            ConnectorSession connectorSession = session.toConnectorSession(function.catalogHandle());
            CharVarcharCoercion charVarcharCoercion = getCharVarcharCoercion(session);
            return new BoundPreimage(projection, new Context(connectorSession, function.signature(), argument, constants, exactness, new PreimageFunctionDependencies()
            {
                @Override
                public Object invoke(List<Object> values)
                {
                    return invoker(function).invoke(connectorSession, values);
                }

                @Override
                public Optional<Function<Object, Object>> coercion(Type source, Type target)
                {
                    return FunctionPreimages.this.coercion(charVarcharCoercion, source, target).map(coercion -> {
                        Invoker invoker = invoker(coercion);
                        ConnectorSession coercionSession = session.toConnectorSession(coercion.catalogHandle());
                        return value -> invoker.invoke(coercionSession, List.of(value));
                    });
                }

                @Override
                public Comparator<Object> resultComparator()
                {
                    return comparator(function.signature().getReturnType());
                }

                @Override
                public boolean canCoerce(Type source, Type target)
                {
                    return typeCoercions.computeIfAbsent(charVarcharCoercion, policy -> new TypeCoercion(typeManager::getType, policy))
                            .canCoerce(source, target);
                }
            }));
        });
    }

    public Optional<BoundPreimage> bindCast(Session session, Type inputType, Type resultType, Exactness exactness)
    {
        return coercion(getCharVarcharCoercion(session), inputType, resultType)
                .flatMap(cast -> bind(session, cast, 0, List.of(Optional.empty()), exactness));
    }

    /// Returns a domain containing every input whose cast result belongs to `resultDomain`.
    /// The domain may contain additional inputs when the exact set cannot be represented.
    /// If no provider supports the cast and requested set, returns all input values.
    public Domain castPreimage(Session session, Domain resultDomain, Type inputType)
    {
        if (inputType.equals(resultDomain.getType())) {
            return resultDomain;
        }
        return bindCast(session, inputType, resultDomain.getType(), CONSERVATIVE)
                .flatMap(bound -> bound.project(resultDomain))
                .map(PreimageResult::inputDomain)
                .orElseGet(() -> Domain.all(inputType));
    }

    private Optional<DomainProjection> projection(Session session, ResolvedFunction function)
    {
        return projections.computeIfAbsent(function, key -> metadata.getDomainProjection(session, key));
    }

    private Optional<ResolvedFunction> coercion(CharVarcharCoercion charVarcharCoercion, Type source, Type target)
    {
        return coercions.computeIfAbsent(new CoercionKey(charVarcharCoercion, source, target), key -> {
            try {
                return Optional.of(metadata.getCoercion(key.charVarcharCoercion(), key.source(), key.target()));
            }
            catch (OperatorNotFoundException _) {
                return Optional.empty();
            }
        });
    }

    private Invoker invoker(ResolvedFunction function)
    {
        return invokers.computeIfAbsent(function, this::createInvoker);
    }

    private Invoker createInvoker(ResolvedFunction function)
    {
        // Preimage functions never accept nulls and never return null for non-null arguments.
        int arity = function.signature().getArgumentTypes().size();
        ScalarFunctionImplementation implementation = functionManager.getScalarFunctionImplementation(
                function,
                new InvocationConvention(nCopies(arity, NEVER_NULL), FAIL_ON_NULL, true, true));
        MethodHandle method = implementation.getMethodHandle();
        if (implementation.getInstanceFactory().isPresent()) {
            try {
                method = method.bindTo(implementation.getInstanceFactory().get().invoke());
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
            }
        }
        boolean session = method.type().parameterCount() > arity && method.type().parameterType(0) == ConnectorSession.class;
        MethodHandle generic = method.asSpreader(Object[].class, arity)
                .asType(session
                        ? methodType(Object.class, ConnectorSession.class, Object[].class)
                        : methodType(Object.class, Object[].class));
        return (connectorSession, arguments) -> {
            try {
                Object[] values = arguments.toArray();
                return session ? generic.invokeExact(connectorSession, values) : generic.invokeExact(values);
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
            }
        };
    }

    private Comparator<Object> comparator(Type type)
    {
        return comparators.computeIfAbsent(type, key -> {
            MethodHandle comparison = typeManager.getTypeOperators()
                    .getComparisonUnorderedLastOperator(key, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL))
                    .asType(methodType(long.class, Object.class, Object.class));
            return (left, right) -> {
                try {
                    return Long.signum((long) comparison.invokeExact(left, right));
                }
                catch (Throwable throwable) {
                    throwIfUnchecked(throwable);
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
                }
            };
        });
    }

    private interface Invoker
    {
        Object invoke(ConnectorSession session, List<Object> arguments);
    }

    private record CoercionKey(CharVarcharCoercion charVarcharCoercion, Type source, Type target) {}

    /// A provider and the call information supplied to each of its requests.
    public record BoundPreimage(DomainProjection projection, Context context)
    {
        public Optional<PreimageResult> project(Domain domain)
        {
            return projection.preimage(context, domain);
        }

        public Optional<NullableValue> comparisonConstant(NullableValue constant)
        {
            return projection.comparisonConstant(context, constant);
        }

        public boolean isComparisonIdentity()
        {
            return projection.isComparisonIdentity(context);
        }
    }
}
