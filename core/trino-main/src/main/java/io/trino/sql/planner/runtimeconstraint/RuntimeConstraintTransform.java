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
package io.trino.sql.planner.runtimeconstraint;

import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.planner.DomainCoercer.applySaturatedCasts;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.NULL_SAFE;
import static java.util.Objects.requireNonNull;

/// The value operation owned by one physical subscription node.
public record RuntimeConstraintTransform(Kind kind, ComparisonOperator operator, boolean nullAllowed, Optional<Type> targetType)
{
    public enum Kind
    {
        IDENTITY,
        COMPARISON,
        CAST,
    }

    public static final RuntimeConstraintTransform IDENTITY = new RuntimeConstraintTransform(Kind.IDENTITY, EQUAL, false, Optional.empty());

    public RuntimeConstraintTransform
    {
        requireNonNull(kind, "kind is null");
        requireNonNull(operator, "operator is null");
        requireNonNull(targetType, "targetType is null");
        checkArgument((kind == Kind.CAST) == targetType.isPresent(), "only cast transformations have a target type");
        checkArgument(!nullAllowed || operator == EQUAL, "nullAllowed requires equality");
    }

    public static RuntimeConstraintTransform comparison(ComparisonOperator operator, boolean nullAllowed)
    {
        return new RuntimeConstraintTransform(Kind.COMPARISON, operator, nullAllowed, Optional.empty());
    }

    public static RuntimeConstraintTransform cast(Type type)
    {
        return new RuntimeConstraintTransform(Kind.CAST, EQUAL, false, Optional.of(type));
    }

    public RuntimeMembershipPayload apply(RuntimeMembershipPayload input, Context context)
    {
        if (kind == Kind.IDENTITY) {
            return input;
        }
        RuntimeMembershipPayload result = new RuntimeMembershipPayload(input.lanes().stream()
                .map(lane -> {
                    Domain domain = lane.domain();
                    switch (kind) {
                        case IDENTITY -> throw new IllegalStateException("identity already handled");
                        case COMPARISON -> {
                            domain = RuntimeConstraintDeriver.applyComparison(domain, operator, nullAllowed, input.sawInputRow(), lane.sawNull());
                            if (input.nullMatchMode() == NULL_SAFE && !input.sawInputRow() && domain.isNone()) {
                                domain = Domain.onlyNull(domain.getType());
                            }
                        }
                        case CAST -> {
                            Type type = targetType.orElseThrow();
                            if (!domain.getType().equals(type)) {
                                requireNonNull(context, "cast transformation context is missing");
                                domain = applySaturatedCasts(context.metadata(), context.functionManager(), context.typeOperators(), context.session(), domain, type);
                            }
                        }
                    }
                    return new Lane(domain, lane.sawNull());
                })
                .toList(), input.nullMatchMode(), input.sawInputRow());
        return result.equals(input) ? input : result;
    }

    public record Context(Metadata metadata, FunctionManager functionManager, TypeOperators typeOperators, Session session)
    {
        public Context
        {
            requireNonNull(metadata, "metadata is null");
            requireNonNull(functionManager, "functionManager is null");
            requireNonNull(typeOperators, "typeOperators is null");
            requireNonNull(session, "session is null");
        }
    }
}
