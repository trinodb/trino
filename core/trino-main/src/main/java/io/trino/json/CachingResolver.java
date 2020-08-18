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
package io.trino.json;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.json.ir.IrPathNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.type.TypeCoercion;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.json.CachingResolver.ResolvedOperatorAndCoercions.RESOLUTION_ERROR;
import static io.trino.json.CachingResolver.ResolvedOperatorAndCoercions.operators;
import static java.util.Objects.requireNonNull;

/**
 * A resolver providing coercions and binary operators used for JSON path evaluation,
 * based on the operation type and the input types.
 * <p>
 * It is instantiated per-driver, and caches the resolved operators and coercions.
 * Caching is applied to IrArithmeticBinary and IrComparisonPredicate path nodes.
 * Its purpose is to avoid resolving operators and coercions on a per-row basis, assuming
 * that the input types to the JSON path operations repeat across rows.
 * <p>
 * If an operator or a component coercion cannot be resolved for certain input types,
 * it is cached as RESOLUTION_ERROR. It depends on the caller to handle this condition.
 */
public class CachingResolver
{
    private static final int MAX_CACHE_SIZE = 1000;

    private final Metadata metadata;
    private final Session session;
    private final TypeCoercion typeCoercion;
    private final NonEvictableCache<NodeAndTypes, ResolvedOperatorAndCoercions> operators = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE));

    public CachingResolver(Metadata metadata, ConnectorSession connectorSession, TypeManager typeManager)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(connectorSession, "connectorSession is null");
        requireNonNull(typeManager, "typeManager is null");

        this.metadata = metadata;
        this.session = ((FullConnectorSession) connectorSession).getSession();
        this.typeCoercion = new TypeCoercion(typeManager::getType);
    }

    public ResolvedOperatorAndCoercions getOperators(IrPathNode node, OperatorType operatorType, Type leftType, Type rightType)
    {
        try {
            return operators
                    .get(new NodeAndTypes(IrPathNodeRef.of(node), leftType, rightType), () -> resolveOperators(operatorType, leftType, rightType));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private ResolvedOperatorAndCoercions resolveOperators(OperatorType operatorType, Type leftType, Type rightType)
    {
        ResolvedFunction operator;
        try {
            operator = metadata.resolveOperator(session, operatorType, ImmutableList.of(leftType, rightType));
        }
        catch (OperatorNotFoundException e) {
            return RESOLUTION_ERROR;
        }

        BoundSignature signature = operator.getSignature();

        Optional<ResolvedFunction> leftCast = Optional.empty();
        if (!signature.getArgumentTypes().get(0).equals(leftType) && !typeCoercion.isTypeOnlyCoercion(leftType, signature.getArgumentTypes().get(0))) {
            try {
                leftCast = Optional.of(metadata.getCoercion(session, leftType, signature.getArgumentTypes().get(0)));
            }
            catch (OperatorNotFoundException e) {
                return RESOLUTION_ERROR;
            }
        }

        Optional<ResolvedFunction> rightCast = Optional.empty();
        if (!signature.getArgumentTypes().get(1).equals(rightType) && !typeCoercion.isTypeOnlyCoercion(rightType, signature.getArgumentTypes().get(1))) {
            try {
                rightCast = Optional.of(metadata.getCoercion(session, rightType, signature.getArgumentTypes().get(1)));
            }
            catch (OperatorNotFoundException e) {
                return RESOLUTION_ERROR;
            }
        }

        return operators(operator, leftCast, rightCast);
    }

    private static class NodeAndTypes
    {
        private final IrPathNodeRef<IrPathNode> node;
        private final Type leftType;
        private final Type rightType;

        public NodeAndTypes(IrPathNodeRef<IrPathNode> node, Type leftType, Type rightType)
        {
            this.node = node;
            this.leftType = leftType;
            this.rightType = rightType;
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
            NodeAndTypes that = (NodeAndTypes) o;
            return Objects.equals(node, that.node) &&
                    Objects.equals(leftType, that.leftType) &&
                    Objects.equals(rightType, that.rightType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(node, leftType, rightType);
        }
    }

    public static class ResolvedOperatorAndCoercions
    {
        public static final ResolvedOperatorAndCoercions RESOLUTION_ERROR = new ResolvedOperatorAndCoercions(null, Optional.empty(), Optional.empty());

        private final ResolvedFunction operator;
        private final Optional<ResolvedFunction> leftCoercion;
        private final Optional<ResolvedFunction> rightCoercion;

        public static ResolvedOperatorAndCoercions operators(ResolvedFunction operator, Optional<ResolvedFunction> leftCoercion, Optional<ResolvedFunction> rightCoercion)
        {
            return new ResolvedOperatorAndCoercions(requireNonNull(operator, "operator is null"), leftCoercion, rightCoercion);
        }

        private ResolvedOperatorAndCoercions(ResolvedFunction operator, Optional<ResolvedFunction> leftCoercion, Optional<ResolvedFunction> rightCoercion)
        {
            this.operator = operator;
            this.leftCoercion = requireNonNull(leftCoercion, "leftCoercion is null");
            this.rightCoercion = requireNonNull(rightCoercion, "rightCoercion is null");
        }

        public ResolvedFunction getOperator()
        {
            checkState(this != RESOLUTION_ERROR, "accessing operator on RESOLUTION_ERROR");
            return operator;
        }

        public Optional<ResolvedFunction> getLeftCoercion()
        {
            checkState(this != RESOLUTION_ERROR, "accessing coercion on RESOLUTION_ERROR");
            return leftCoercion;
        }

        public Optional<ResolvedFunction> getRightCoercion()
        {
            checkState(this != RESOLUTION_ERROR, "accessing coercion on RESOLUTION_ERROR");
            return rightCoercion;
        }
    }
}
