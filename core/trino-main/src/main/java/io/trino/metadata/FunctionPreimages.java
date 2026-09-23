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
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.DomainProjection;
import io.trino.spi.function.PreimageFunctionDependencies;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.function.PreimageResult.Exactness;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.type.TypeCoercion;

import java.lang.invoke.MethodHandle;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

/// Finds input values for which a scalar function produces a result in a requested set.
/// For example, the preimage of the year 2025 through `year(date)` is the set of dates in 2025.
/// The function's registered provider supplies this knowledge. This class supplies the
/// session, concrete argument and result types, fixed argument values, and operations
/// the provider can use to evaluate the function or convert boundary values.
public final class FunctionPreimages
{
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final TypeManager typeManager;
    private final Session session;

    public FunctionPreimages(Metadata metadata, FunctionManager functionManager, TypeManager typeManager, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.session = requireNonNull(session, "session is null");
    }

    /// Pairs a registered provider with the information for one function call. The returned
    /// object reuses that information for requests about different result sets or constants.
    /// An empty result means that the function is nondeterministic or has no registered provider.
    public Optional<BoundPreimage> bind(ResolvedFunction function, int argument, List<Optional<NullableValue>> constants, Exactness exactness)
    {
        if (!function.deterministic()) {
            return Optional.empty();
        }
        return metadata.getDomainProjection(session, function).map(projection -> new BoundPreimage(projection,
                new Context(session.toConnectorSession(function.catalogHandle()), function.signature(), argument, constants, exactness, new PreimageFunctionDependencies()
                {
                    private final InterpretedFunctionInvoker invoker = new InterpretedFunctionInvoker(functionManager);
                    private final Supplier<Comparator<Object>> resultComparator = memoize(() -> FunctionPreimages.this.resultComparator(function.signature().getReturnType()));

                    @Override
                    public Object invoke(List<Object> values)
                    {
                        return invoker.invoke(function, session.toConnectorSession(function.catalogHandle()), values);
                    }

                    @Override
                    public Optional<Function<Object, Object>> coercion(Type source, Type target)
                    {
                        try {
                            ResolvedFunction coercion = metadata.getCoercion(getCharVarcharCoercion(session), source, target);
                            return Optional.of(value -> invoker.invoke(coercion, session.toConnectorSession(coercion.catalogHandle()), value));
                        }
                        catch (OperatorNotFoundException _) {
                            return Optional.empty();
                        }
                    }

                    @Override
                    public Comparator<Object> resultComparator()
                    {
                        return resultComparator.get();
                    }

                    @Override
                    public boolean canCoerce(Type source, Type target)
                    {
                        return new TypeCoercion(typeManager::getType, getCharVarcharCoercion(session)).canCoerce(source, target);
                    }
                })));
    }

    private Comparator<Object> resultComparator(Type type)
    {
        MethodHandle comparison = typeManager.getTypeOperators()
                .getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL))
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
    }

    public Optional<BoundPreimage> bindCast(Type inputType, Type resultType, Exactness exactness)
    {
        try {
            return bind(metadata.getCoercion(getCharVarcharCoercion(session), inputType, resultType), 0, List.of(Optional.empty()), exactness);
        }
        catch (OperatorNotFoundException _) {
            return Optional.empty();
        }
    }

    /// Returns a domain containing every input whose cast result belongs to `resultDomain`.
    /// The domain may contain additional inputs when the exact set cannot be represented.
    /// If no provider supports the cast and requested set, returns all input values.
    public Domain castPreimage(Domain resultDomain, Type inputType)
    {
        if (inputType.equals(resultDomain.getType())) {
            return resultDomain;
        }
        return bindCast(inputType, resultDomain.getType(), CONSERVATIVE)
                .flatMap(bound -> bound.project(resultDomain))
                .map(PreimageResult::inputDomain)
                .orElseGet(() -> Domain.all(inputType));
    }

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
