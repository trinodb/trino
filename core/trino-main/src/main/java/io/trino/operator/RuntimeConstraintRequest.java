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
package io.trino.operator;

import io.trino.spi.type.Type;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record RuntimeConstraintRequest(
        RuntimeConstraintId constraintId,
        int channel,
        Kind kind,
        ComparisonOperator operator,
        boolean nullAllowed,
        Optional<Type> targetType,
        OptionalInt relatedChannel,
        Optional<RuntimeConstraintId> subscriptionId)
{
    private static final RuntimeConstraintId TRUE_DEMAND_ID = new RuntimeConstraintId("$require_true");
    private static final RuntimeConstraintId COMPARISON_DEMAND_ID = new RuntimeConstraintId("$comparison");
    private static final String COLLECTION_SOURCE_PREFIX = "$runtime_constraint_collection_";

    public RuntimeConstraintRequest(
            RuntimeConstraintId constraintId,
            int channel,
            Kind kind,
            ComparisonOperator operator,
            boolean nullAllowed,
            Optional<Type> targetType,
            OptionalInt relatedChannel)
    {
        this(constraintId, channel, kind, operator, nullAllowed, targetType, relatedChannel, Optional.empty());
    }

    public RuntimeConstraintId subscription()
    {
        return subscriptionId.orElse(constraintId);
    }

    public RuntimeConstraintRequest withSubscription(RuntimeConstraintId id)
    {
        checkArgument(isConstraint(), "only constraints have subscriptions");
        return new RuntimeConstraintRequest(constraintId, channel, kind, operator, nullAllowed, targetType, relatedChannel, Optional.of(id));
    }

    public RuntimeConstraintRequest(RuntimeConstraintId constraintId, int channel)
    {
        this(constraintId, channel, Kind.CONSTRAINT, ComparisonOperator.EQUAL, false, Optional.empty(), OptionalInt.empty());
    }

    public RuntimeConstraintRequest(
            RuntimeConstraintId constraintId,
            int channel,
            ComparisonOperator operator,
            boolean nullAllowed,
            Type targetType)
    {
        this(constraintId, channel, Kind.CONSTRAINT, operator, nullAllowed, Optional.of(targetType), OptionalInt.empty());
    }

    public RuntimeConstraintRequest
    {
        requireNonNull(constraintId, "constraintId is null");
        requireNonNull(kind, "kind is null");
        requireNonNull(operator, "operator is null");
        requireNonNull(targetType, "targetType is null");
        requireNonNull(relatedChannel, "relatedChannel is null");
        subscriptionId = subscriptionId == null ? Optional.empty() : subscriptionId;
        checkArgument(subscriptionId.isEmpty() || kind == Kind.CONSTRAINT, "only constraints have subscriptions");
        checkArgument(channel >= 0, "channel is negative");
        checkArgument(!nullAllowed || operator == ComparisonOperator.EQUAL, "nullAllowed requires equality");
        checkArgument(kind == Kind.COMPARISON_DEMAND ^ relatedChannel.isEmpty(), "relatedChannel must be present only for comparison demands");
        relatedChannel.ifPresent(value -> checkArgument(value >= 0, "relatedChannel is negative"));
    }

    public RuntimeConstraintRequest withChannel(int channel)
    {
        checkArgument(!isComparisonDemand(), "comparison demand requires both channels to be mapped");
        return new RuntimeConstraintRequest(constraintId, channel, kind, operator, nullAllowed, targetType, relatedChannel, subscriptionId);
    }

    public RuntimeConstraintRequest mapChannels(IntUnaryOperator mapping)
    {
        requireNonNull(mapping, "mapping is null");
        return new RuntimeConstraintRequest(
                constraintId,
                mapping.applyAsInt(channel),
                kind,
                operator,
                nullAllowed,
                targetType,
                relatedChannel.isPresent() ? OptionalInt.of(mapping.applyAsInt(relatedChannel.orElseThrow())) : OptionalInt.empty(),
                subscriptionId);
    }

    public boolean channelsMatch(IntPredicate predicate)
    {
        requireNonNull(predicate, "predicate is null");
        return predicate.test(channel) && (relatedChannel.isEmpty() || predicate.test(relatedChannel.orElseThrow()));
    }

    public RuntimeConstraintRequest withChannelAndTargetType(int channel, Type targetType)
    {
        return new RuntimeConstraintRequest(constraintId, channel, kind, operator, nullAllowed, Optional.of(requireNonNull(targetType, "targetType is null")), relatedChannel, subscriptionId);
    }

    public static RuntimeConstraintRequest requireTrue(int channel)
    {
        return new RuntimeConstraintRequest(TRUE_DEMAND_ID, channel, Kind.REQUIRE_TRUE, ComparisonOperator.EQUAL, false, Optional.empty(), OptionalInt.empty());
    }

    public static RuntimeConstraintRequest comparisonDemand(int leftChannel, int rightChannel, ComparisonOperator operator, boolean nullAllowed)
    {
        return new RuntimeConstraintRequest(COMPARISON_DEMAND_ID, leftChannel, Kind.COMPARISON_DEMAND, operator, nullAllowed, Optional.empty(), OptionalInt.of(rightChannel));
    }

    public static RuntimeConstraintRequest collection(
            RuntimeConstraintId constraintId,
            int channel,
            ComparisonOperator operator,
            boolean nullAllowed,
            Type targetType,
            boolean replicated)
    {
        return new RuntimeConstraintRequest(constraintId, channel, replicated ? Kind.REPLICATED_COLLECTION : Kind.COLLECTION, operator, nullAllowed, Optional.of(targetType), OptionalInt.empty());
    }

    public boolean isConstraint()
    {
        return kind == Kind.CONSTRAINT;
    }

    public boolean isComparisonDemand()
    {
        return kind == Kind.COMPARISON_DEMAND;
    }

    public boolean isCollection()
    {
        return kind == Kind.COLLECTION || kind == Kind.REPLICATED_COLLECTION;
    }

    public boolean isReplicatedCollection()
    {
        return kind == Kind.REPLICATED_COLLECTION;
    }

    public PlanNodeId collectionSourceId()
    {
        checkArgument(isCollection(), "request is not a collection request");
        return new PlanNodeId(COLLECTION_SOURCE_PREFIX + constraintId);
    }

    public static RuntimeConstraintId joinConstraintId(PlanNodeId joinId, int keyIndex)
    {
        requireNonNull(joinId, "joinId is null");
        checkArgument(keyIndex >= 0, "keyIndex is negative");
        return new RuntimeConstraintId("join_" + joinId + "_constraint_" + keyIndex);
    }

    public static RuntimeConstraintId semiJoinConstraintId(PlanNodeId semiJoinId)
    {
        requireNonNull(semiJoinId, "semiJoinId is null");
        return new RuntimeConstraintId("semijoin_" + semiJoinId + "_constraint");
    }

    public enum Kind
    {
        CONSTRAINT,
        COLLECTION,
        REPLICATED_COLLECTION,
        REQUIRE_TRUE,
        COMPARISON_DEMAND,
    }
}
