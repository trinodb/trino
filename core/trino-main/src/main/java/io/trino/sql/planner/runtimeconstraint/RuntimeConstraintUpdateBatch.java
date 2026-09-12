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
public record RuntimeConstraintUpdateBatch(
        int formatVersion,
        long sequence,
        long generation,
        List<RuntimeConstraintSnapshot> snapshots)
{
    public RuntimeConstraintUpdateBatch
    {
        checkArgument(formatVersion > 0, "formatVersion must be positive");
        checkArgument(sequence >= 0, "sequence is negative");
        checkArgument(generation >= 0, "generation is negative");
        snapshots = ImmutableList.copyOf(requireNonNull(snapshots, "snapshots is null"));
        checkArgument(sequence > 0 || snapshots.isEmpty(), "initial batch contains snapshots");
        checkArgument(snapshots.stream().allMatch(snapshot -> snapshot.generation() == generation), "snapshot generation does not match batch generation");
        checkArgument(snapshots.stream().map(RuntimeConstraintSnapshot::constraintId).distinct().count() == snapshots.size(), "batch contains duplicate constraints");
    }

    public static RuntimeConstraintUpdateBatch empty(long generation)
    {
        return new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 0, generation, ImmutableList.of());
    }
}
