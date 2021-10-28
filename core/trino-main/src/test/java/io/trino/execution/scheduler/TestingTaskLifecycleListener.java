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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.trino.execution.RemoteTask;
import io.trino.execution.TaskId;
import io.trino.sql.planner.plan.PlanFragmentId;

import javax.annotation.concurrent.GuardedBy;

import java.util.Set;

import static com.google.common.collect.Sets.newConcurrentHashSet;

public class TestingTaskLifecycleListener
        implements TaskLifecycleListener
{
    @GuardedBy("this")
    private final Multimap<PlanFragmentId, TaskId> tasks = ArrayListMultimap.create();
    private final Set<PlanFragmentId> noMoreTasks = newConcurrentHashSet();

    @Override
    public synchronized void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
    {
        tasks.put(fragmentId, task.getTaskId());
    }

    public synchronized Multimap<PlanFragmentId, TaskId> getTasks()
    {
        return ImmutableListMultimap.copyOf(tasks);
    }

    @Override
    public void noMoreTasks(PlanFragmentId fragmentId)
    {
        noMoreTasks.add(fragmentId);
    }

    public Set<PlanFragmentId> getNoMoreTasks()
    {
        return ImmutableSet.copyOf(noMoreTasks);
    }
}
