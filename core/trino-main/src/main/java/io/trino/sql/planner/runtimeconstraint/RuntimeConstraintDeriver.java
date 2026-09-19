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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.NULL_SAFE;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static java.util.Objects.requireNonNull;

public final class RuntimeConstraintDeriver
{
    private RuntimeConstraintDeriver() {}

    public static Map<RuntimeConstraintId, RuntimeMembershipPayload> derive(
            RuntimeConstraintProducerGroup group,
            List<RuntimeConstraintPayload> contributions)
    {
        requireNonNull(group, "group is null");
        requireNonNull(contributions, "contributions is null");

        RuntimeMembershipPayload merged;
        if (contributions.isEmpty()) {
            merged = new RuntimeMembershipPayload(
                    group.collectedLanes().stream().map(lane -> new Lane(Domain.none(lane.type()), false)).toList(),
                    ORDINARY,
                    false);
        }
        else {
            List<RuntimeMembershipPayload> payloads = contributions.stream()
                    .map(payload -> {
                        checkArgument(payload instanceof RuntimeMembershipPayload, "unsupported membership contribution payload: %s", payload.getClass().getSimpleName());
                        return (RuntimeMembershipPayload) payload;
                    })
                    .toList();
            int laneCount = group.collectedLanes().size();
            checkArgument(payloads.stream().allMatch(payload -> payload.lanes().size() == laneCount), "membership contribution has wrong lane count");
            checkArgument(payloads.stream().map(RuntimeMembershipPayload::nullMatchMode).distinct().count() == 1, "membership contributions have different null modes");
            merged = new RuntimeMembershipPayload(
                    IntStream.range(0, laneCount)
                            .mapToObj(index -> new Lane(
                                    Domain.union(payloads.stream().map(payload -> payload.lanes().get(index).domain()).toList()),
                                    payloads.stream().anyMatch(payload -> payload.lanes().get(index).sawNull())))
                            .toList(),
                    payloads.getFirst().nullMatchMode(),
                    payloads.stream().anyMatch(RuntimeMembershipPayload::sawInputRow));
        }

        ImmutableMap.Builder<RuntimeConstraintId, RuntimeMembershipPayload> derivedPayloads = ImmutableMap.builder();
        for (RuntimeConstraintDerivation derivation : group.derivations()) {
            List<Lane> lanes = derivation.collectedLaneIndexes().stream()
                    .map(merged.lanes()::get)
                    .map(lane -> new Lane(
                            applyComparison(lane.domain(), derivation.operator(), derivation.nullAllowed(), merged.sawInputRow(), lane.sawNull()),
                            lane.sawNull()))
                    .toList();
            derivedPayloads.put(derivation.constraintId(), new RuntimeMembershipPayload(
                    lanes,
                    derivation.nullAllowed() ? NULL_SAFE : ORDINARY,
                    merged.sawInputRow()));
        }
        return derivedPayloads.buildOrThrow();
    }

    public static Domain applyComparison(Domain domain, ComparisonOperator operator, boolean nullAllowed, boolean sawInputRow, boolean sawNull)
    {
        if (domain.isAll()) {
            return domain;
        }
        if (!sawInputRow) {
            return Domain.none(domain.getType());
        }
        if (domain.isNone()) {
            return nullAllowed && sawNull ? Domain.onlyNull(domain.getType()) : domain;
        }
        if (operator == EQUAL) {
            return nullAllowed ? Domain.create(domain.getValues(), sawNull) : domain;
        }
        Range span = domain.getValues().getRanges().getSpan();
        return switch (operator) {
            case LESS_THAN -> Domain.create(ValueSet.ofRanges(Range.lessThan(span.getType(), span.getHighBoundedValue())), false);
            case LESS_THAN_OR_EQUAL -> Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(span.getType(), span.getHighBoundedValue())), false);
            case GREATER_THAN -> Domain.create(ValueSet.ofRanges(Range.greaterThan(span.getType(), span.getLowBoundedValue())), false);
            case GREATER_THAN_OR_EQUAL -> Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(span.getType(), span.getLowBoundedValue())), false);
            case EQUAL, NOT_EQUAL, IDENTICAL -> throw new IllegalArgumentException("unsupported runtime constraint comparison operator: " + operator);
        };
    }
}
