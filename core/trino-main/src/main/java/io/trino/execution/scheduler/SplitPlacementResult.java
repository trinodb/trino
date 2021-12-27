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
package io.trino.execution.scheduler;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;

import static java.util.Objects.requireNonNull;

public final class SplitPlacementResult
{
    private final ListenableFuture<Void> blocked;
    private final Multimap<InternalNode, Split> assignments;

    public SplitPlacementResult(ListenableFuture<Void> blocked, Multimap<InternalNode, Split> assignments)
    {
        this.blocked = requireNonNull(blocked, "blocked is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
    }

    public ListenableFuture<Void> getBlocked()
    {
        return blocked;
    }

    public Multimap<InternalNode, Split> getAssignments()
    {
        return assignments;
    }
}
