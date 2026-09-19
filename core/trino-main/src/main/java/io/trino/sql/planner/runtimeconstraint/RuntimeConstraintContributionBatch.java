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

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public record RuntimeConstraintContributionBatch(
        int formatVersion,
        long sequence,
        long generation,
        List<RuntimeConstraintContribution> contributions)
{
    public RuntimeConstraintContributionBatch
    {
        checkArgument(formatVersion > 0, "formatVersion must be positive");
        checkArgument(sequence >= 0, "sequence is negative");
        checkArgument(generation >= 0, "generation is negative");
        contributions = ImmutableList.copyOf(requireNonNull(contributions, "contributions is null"));
        checkArgument(sequence > 0 || contributions.isEmpty(), "initial batch contains contributions");
    }

    public static RuntimeConstraintContributionBatch empty(long generation)
    {
        return new RuntimeConstraintContributionBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 0, generation, ImmutableList.of());
    }

    public RuntimeConstraintContributionBatch withGeneration(long generation)
    {
        return new RuntimeConstraintContributionBatch(formatVersion, sequence, generation, contributions);
    }
}
