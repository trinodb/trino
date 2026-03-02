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
package io.trino.execution;

import com.google.common.primitives.Longs;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

import static java.util.Objects.requireNonNull;

public record ScheduledSplit(long sequenceId, PlanNodeId planNodeId, Split split)
{
    public ScheduledSplit
    {
        requireNonNull(planNodeId, "planNodeId is null");
    }

    @Override
    public int hashCode()
    {
        return Longs.hashCode(sequenceId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ScheduledSplit other = (ScheduledSplit) obj;
        return this.sequenceId == other.sequenceId;
    }
}
