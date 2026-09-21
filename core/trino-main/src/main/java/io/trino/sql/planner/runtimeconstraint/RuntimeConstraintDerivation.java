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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.ir.ComparisonOperator;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public record RuntimeConstraintDerivation(
        RuntimeConstraintId constraintId,
        RuntimeConstraintKind kind,
        List<Integer> collectedLaneIndexes,
        ComparisonOperator operator,
        boolean nullAllowed)
{
    public RuntimeConstraintDerivation(RuntimeConstraintId constraintId, RuntimeConstraintKind kind, List<Integer> collectedLaneIndexes)
    {
        this(constraintId, kind, collectedLaneIndexes, ComparisonOperator.EQUAL, false);
    }

    public RuntimeConstraintDerivation
    {
        requireNonNull(constraintId, "constraintId is null");
        requireNonNull(kind, "kind is null");
        collectedLaneIndexes = ImmutableList.copyOf(requireNonNull(collectedLaneIndexes, "collectedLaneIndexes is null"));
        checkArgument(!collectedLaneIndexes.isEmpty(), "collectedLaneIndexes is empty");
        checkArgument(collectedLaneIndexes.stream().allMatch(index -> index >= 0), "collectedLaneIndexes contains a negative index");
        requireNonNull(operator, "operator is null");
        checkArgument(!nullAllowed || operator == ComparisonOperator.EQUAL, "nullAllowed requires equality comparison");
    }
}
