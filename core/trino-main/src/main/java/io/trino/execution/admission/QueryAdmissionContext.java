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
package io.trino.execution.admission;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.QueryId;

import static java.util.Objects.requireNonNull;

/**
 * Per-query context handed to the {@link ResourceAwareAdmissionController}. The engine resolves the
 * effective thresholds (session override falling back to the cluster-wide config default)
 * before invoking the policy, keeping the policy a pure function of context and observed capacity.
 *
 * @param queryId the query being admitted
 * @param requiredFreeMemory minimum cluster-wide free memory required before admitting
 * @param requiredFreeVcpu minimum cluster-wide free vCPU required before admitting
 * @param maxWait how long the query may be held before failing with insufficient resources
 */
public record QueryAdmissionContext(
        QueryId queryId,
        DataSize requiredFreeMemory,
        int requiredFreeVcpu,
        Duration maxWait)
{
    public QueryAdmissionContext
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(requiredFreeMemory, "requiredFreeMemory is null");
        requireNonNull(maxWait, "maxWait is null");
        if (requiredFreeVcpu < 0) {
            throw new IllegalArgumentException("requiredFreeVcpu must not be negative");
        }
    }
}
