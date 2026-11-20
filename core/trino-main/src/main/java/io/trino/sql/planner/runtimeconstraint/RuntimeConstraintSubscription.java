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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/// A factory-owned edge that receives an input snapshot and publishes its transformed value.
public record RuntimeConstraintSubscription(
        RuntimeConstraintId id,
        RuntimeConstraintId inputId,
        RuntimeConstraintId constraintId,
        String owner,
        RuntimeConstraintTransform transform)
{
    public RuntimeConstraintSubscription
    {
        requireNonNull(id, "id is null");
        requireNonNull(inputId, "inputId is null");
        requireNonNull(constraintId, "constraintId is null");
        requireNonNull(owner, "owner is null");
        requireNonNull(transform, "transform is null");
        checkArgument(!id.equals(inputId), "subscription cannot depend on itself");
    }

    public static RuntimeConstraintSubscription create(RuntimeConstraintId inputId, RuntimeConstraintId constraintId, String owner, RuntimeConstraintTransform transform)
    {
        String identity = inputId + "\n" + constraintId + "\n" + owner + "\n" + transform;
        return new RuntimeConstraintSubscription(
                new RuntimeConstraintId("subscription_" + sha256().hashString(identity, UTF_8)),
                inputId,
                constraintId,
                owner,
                transform);
    }

    public record Input(RuntimeConstraintId id, RuntimeConstraintId constraintId)
    {
        public Input
        {
            requireNonNull(id, "id is null");
            requireNonNull(constraintId, "constraintId is null");
        }
    }
}
