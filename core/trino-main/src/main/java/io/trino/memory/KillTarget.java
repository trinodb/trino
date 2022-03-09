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
package io.trino.memory;

import com.google.common.collect.ImmutableSet;
import io.trino.execution.TaskId;
import io.trino.spi.QueryId;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class KillTarget
{
    // Either query id or tasks list must be set
    // If query id is set then whole query will be killed; individual tasks otherwise.

    private final Optional<QueryId> query;
    private final Set<TaskId> tasks;

    public static KillTarget wholeQuery(QueryId queryId)
    {
        return new KillTarget(Optional.of(queryId), ImmutableSet.of());
    }

    public static KillTarget selectedTasks(Set<TaskId> tasks)
    {
        return new KillTarget(Optional.empty(), tasks);
    }

    private KillTarget(Optional<QueryId> query, Set<TaskId> tasks)
    {
        requireNonNull(query, "query is null");
        requireNonNull(tasks, "tasks is null");
        if ((query.isPresent() && !tasks.isEmpty()) || (query.isEmpty() && tasks.isEmpty())) {
            throw new IllegalArgumentException("either query or tasks must be set");
        }
        this.query = query;
        this.tasks = ImmutableSet.copyOf(tasks);
    }

    public boolean isWholeQuery()
    {
        return query.isPresent();
    }

    public QueryId getQuery()
    {
        return query.orElseThrow(() -> new IllegalStateException("query not set in KillTarget: " + this));
    }

    public Set<TaskId> getTasks()
    {
        checkState(!tasks.isEmpty(), "tasks not set in KillTarget: " + this);
        return tasks;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KillTarget that = (KillTarget) o;
        return Objects.equals(query, that.query) && Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, tasks);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", query)
                .add("tasks", tasks)
                .toString();
    }
}
