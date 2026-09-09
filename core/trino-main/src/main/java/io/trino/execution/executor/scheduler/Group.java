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
package io.trino.execution.executor.scheduler;

import jakarta.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/// A scheduling group. Groups nest: a group with a `parent` is a child of that parent in the
/// scheduling tree, so fairness can be enforced across several levels (e.g. task, then pipeline).
public record Group(@Nullable Group parent, String name, long startTime)
{
    public Group(String name)
    {
        this(null, name, System.nanoTime());
    }

    public Group(Group parent, String name)
    {
        this(parent, name, System.nanoTime());
    }

    /// The chain of groups from the top-level ancestor down to this group.
    public List<Group> path()
    {
        Deque<Group> path = new ArrayDeque<>();
        for (Group group = this; group != null; group = group.parent) {
            path.addFirst(group);
        }
        return List.copyOf(path);
    }

    @Override
    public String toString()
    {
        return name;
    }
}
