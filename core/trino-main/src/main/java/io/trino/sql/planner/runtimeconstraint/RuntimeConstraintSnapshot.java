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

import com.google.errorprone.annotations.Immutable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.FINAL;
import static java.util.Objects.requireNonNull;

@Immutable
public record RuntimeConstraintSnapshot(
        RuntimeConstraintId constraintId,
        long generation,
        long version,
        RuntimeConstraintPublicationState state,
        Optional<RuntimeConstraintPayload> payload)
{
    public RuntimeConstraintSnapshot
    {
        requireNonNull(constraintId, "constraintId is null");
        checkArgument(generation >= 0, "generation is negative");
        checkArgument(version >= 0, "version is negative");
        requireNonNull(state, "state is null");
        requireNonNull(payload, "payload is null");
        checkArgument((state == FINAL) == payload.isPresent(), "only final snapshots have a payload");
    }

    public long retainedSizeInBytes()
    {
        return payload.map(RuntimeConstraintPayload::getRetainedSizeInBytes).orElse(0L);
    }

    public static RuntimeConstraintSnapshot pending(RuntimeConstraintId constraintId, long generation)
    {
        return new RuntimeConstraintSnapshot(constraintId, generation, 0, RuntimeConstraintPublicationState.PENDING, Optional.empty());
    }

    public static RuntimeConstraintSnapshot terminal(
            RuntimeConstraintId constraintId,
            long generation,
            long version,
            RuntimeConstraintPublicationState state)
    {
        checkArgument(state != FINAL && state != RuntimeConstraintPublicationState.PENDING, "state is not terminal without a payload");
        return new RuntimeConstraintSnapshot(constraintId, generation, version, state, Optional.empty());
    }

    public static RuntimeConstraintSnapshot finalSnapshot(
            RuntimeConstraintId constraintId,
            long generation,
            long version,
            RuntimeConstraintPayload payload)
    {
        return new RuntimeConstraintSnapshot(
                constraintId,
                generation,
                version,
                FINAL,
                Optional.of(payload));
    }

    public RuntimeConstraintSnapshot withGeneration(long generation)
    {
        return new RuntimeConstraintSnapshot(constraintId, generation, version, state, payload);
    }
}
