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

import com.google.common.collect.ImmutableSet;
import io.trino.annotation.NotThreadSafe;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.execution.executor.scheduler.State.BLOCKED;
import static io.trino.execution.executor.scheduler.State.RUNNABLE;
import static io.trino.execution.executor.scheduler.State.RUNNING;

/**
 * <p>A queue of tasks that are scheduled for execution. Modeled after
 * <a href="https://en.wikipedia.org/wiki/Completely_Fair_Scheduler">Completely Fair Scheduler</a>.
 * Tasks are grouped into scheduling groups. Within a group, tasks are ordered based
 * on their relative weight. Groups are ordered relative to each other based on the
 * accumulated weight of their tasks.</p>
 *
 * <p>A task can be in one of three states:
 * <ul>
 *     <li><b>runnable</b>: the task is ready to run and waiting to be dequeued
 *     <li><b>running</b>: the task has been dequeued and is running
 *     <li><b>blocked</b>: the task is blocked on some external event and is not running
 * </ul>
 * </p>
 * <p>
 * A group can be in one of three states:
 * <ul>
 *     <li><b>runnable</b>: the group has at least one runnable task
 *     <li><b>running</b>: all the tasks in the group are currently running
 *     <li><b>blocked</b>: all the tasks in the group are currently blocked
 * </ul>
 * </p>
 * <p>
 * The goal is to balance the consideration among groups to ensure the accumulated
 * weight in the long run is equal among groups. Within a group, the goal is to
 * balance the consideration among tasks to ensure the accumulated weight in the
 * long run is equal among tasks within the group.
 *
 * <p>Groups start in the blocked state and transition to the runnable state when a task is
 * added via the {@link #enqueue(Object, Object, long)} method.</p>
 *
 * <p>Tasks are dequeued via the {@link #dequeue(long)}. When all tasks in a group have
 * been dequeued, the group transitions to the running state and is removed from the
 * queue.</p>
 *
 * <p>When a task time slice completes, it needs to be re-enqueued via the
 * {@link #enqueue(Object, Object, long)}, which includes the desired
 * increment in relative weight to apply to the task for further prioritization.
 * The weight increment is also applied to the group.
 * </p>
 *
 * <p>If a task blocks, the caller must call the {@link #block(Object, Object, long)}
 * method to indicate that the task is no longer running. A weight increment can be
 * included for the portion of time the task was not blocked.</p>
 * <br/>
 * <h2>Group state transitions</h2>
 * <pre>
 *                                                                 blockTask()
 *    finishTask()               enqueueTask()                     enqueueTask()
 *        ┌───┐   ┌──────────────────────────────────────────┐       ┌────┐
 *        │   │   │                                          │       │    │
 *        │   ▼   │                                          ▼       ▼    │
 *      ┌─┴───────┴─┐   all blocked        finishTask()   ┌────────────┐  │
 *      │           │◄──────────────O◄────────────────────┤            ├──┘
 * ────►│  BLOCKED  │               │                     │  RUNNABLE  │
 *      │           │               │   ┌────────────────►│            │◄───┐
 *      └───────────┘       not all │   │  enqueueTask()  └──────┬─────┘    │
 *            ▲             blocked │   │                        │          │
 *            │                     │   │           dequeueTask()│          │
 *            │ all blocked         ▼   │                        │          │
 *            │                   ┌─────┴─────┐                  ▼          │
 *            │                   │           │◄─────────────────O──────────┘
 *            O◄──────────────────┤  RUNNING  │      queue empty     queue
 *            │      blockTask()  │           ├───┐                 not empty
 *            │                   └───────────┘   │
 *            │                     ▲      ▲      │ finishTask()
 *            └─────────────────────┘      └──────┘
 *                not all blocked
 *
 * </pre>
 *
 * <h2>Implementation notes</h2>
 * <ul>
 *     <li>TODO: Initial weight upon registration</li>
 *     <li>TODO: Weight adjustment during blocking / unblocking</li>
 *     <li>TODO: Uncommitted weight on dequeue</li>
 * </ul>
 * </p>
 */
@NotThreadSafe
final class SchedulingQueue<G, T>
{
    private final PriorityQueue<G> runnableQueue = new PriorityQueue<>();
    private final Map<G, SchedulingGroup<T>> groups = new HashMap<>();
    private final PriorityQueue<G> baselineWeights = new PriorityQueue<>();

    public void startGroup(G group)
    {
        checkArgument(!groups.containsKey(group), "Group already started: %s", group);

        SchedulingGroup<T> info = new SchedulingGroup<>();
        groups.put(group, info);
    }

    public Set<T> finishGroup(G group)
    {
        SchedulingGroup<T> info = groups.remove(group);
        checkArgument(info != null, "Unknown group: %s", group);

        runnableQueue.removeIfPresent(group);
        baselineWeights.removeIfPresent(group);
        return info.tasks();
    }

    public boolean containsGroup(G group)
    {
        return groups.containsKey(group);
    }

    public Set<T> finishAll()
    {
        Set<G> groups = ImmutableSet.copyOf(this.groups.keySet());
        return groups.stream()
                .map(this::finishGroup)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    public void finish(G group, T task)
    {
        checkArgument(groups.containsKey(group), "Unknown group: %s", group);

        SchedulingGroup<T> info = groups.get(group);

        State previousState = info.state();
        info.finish(task);
        State newState = info.state();

        if (newState == RUNNABLE) {
            runnableQueue.addOrReplace(group, info.weight());
            baselineWeights.addOrReplace(group, info.weight());
        }
        else if (newState == RUNNING) {
            runnableQueue.removeIfPresent(group);
            baselineWeights.addOrReplace(group, info.weight());
        }
        else if (newState == BLOCKED && previousState != BLOCKED) {
            info.addWeight(-baselineWeight());
            runnableQueue.removeIfPresent(group);
            baselineWeights.removeIfPresent(group);
        }

        verifyState(group);
    }

    public void enqueue(G group, T task, long deltaWeight)
    {
        checkArgument(groups.containsKey(group), "Unknown group: %s", group);

        SchedulingGroup<T> info = groups.get(group);

        State previousState = info.state();
        info.enqueue(task, deltaWeight);
        verify(info.state() == RUNNABLE);

        if (previousState == BLOCKED) {
            // When transitioning from blocked, set the baseline weight to the minimum current weight
            // to avoid the newly unblocked group from monopolizing the queue while it catches up
            info.addWeight(baselineWeight());
        }

        runnableQueue.addOrReplace(group, info.weight());
        baselineWeights.addOrReplace(group, info.weight());

        verifyState(group);
    }

    public void block(G group, T task, long deltaWeight)
    {
        SchedulingGroup<T> info = groups.get(group);
        checkArgument(info != null, "Unknown group: %s", group);
        checkArgument(info.state() == RUNNABLE || info.state() == RUNNING, "Group is already blocked: %s", group);

        State previousState = info.state();
        info.block(task, deltaWeight);

        doTransition(group, info, previousState, info.state());
    }

    public T dequeue(long expectedWeight)
    {
        G group = runnableQueue.poll();

        if (group == null) {
            return null;
        }

        SchedulingGroup<T> info = groups.get(group);
        verify(info.state() == RUNNABLE, "Group is not runnable: %s", group);

        T task = info.dequeue(expectedWeight);
        verify(task != null);

        baselineWeights.addOrReplace(group, info.weight());
        if (info.state() == RUNNABLE) {
            runnableQueue.add(group, info.weight());
        }

        checkState(info.state() == RUNNABLE || info.state() == RUNNING);
        verifyState(group);

        return task;
    }

    public T peek()
    {
        G group = runnableQueue.peek();

        if (group == null) {
            return null;
        }

        SchedulingGroup<T> info = groups.get(group);
        verify(info.state() == RUNNABLE, "Group is not runnable: %s", group);

        T task = info.peek();
        checkState(task != null);

        return task;
    }

    public int getRunnableCount()
    {
        return runnableQueue.values().stream()
                .map(groups::get)
                .mapToInt(SchedulingGroup::runnableCount)
                .sum();
    }

    public State state(G group)
    {
        SchedulingGroup<T> info = groups.get(group);
        checkArgument(info != null, "Unknown group: %s", group);

        return info.state();
    }

    private long baselineWeight()
    {
        if (baselineWeights.isEmpty()) {
            return 0;
        }

        return baselineWeights.nextPriority();
    }

    private void doTransition(G group, SchedulingGroup<T> info, State previousState, State newState)
    {
        if (newState == RUNNABLE) {
            runnableQueue.addOrReplace(group, info.weight());
            baselineWeights.addOrReplace(group, info.weight());
        }
        else if (newState == RUNNING) {
            runnableQueue.removeIfPresent(group);
            baselineWeights.addOrReplace(group, info.weight());
        }
        else if (newState == BLOCKED && previousState != BLOCKED) {
            info.addWeight(-baselineWeight());
            runnableQueue.removeIfPresent(group);
            baselineWeights.removeIfPresent(group);
        }

        verifyState(group);
    }

    private void verifyState(G groupKey)
    {
        SchedulingGroup<T> group = groups.get(groupKey);
        checkArgument(group != null, "Unknown group: %s", groupKey);

        switch (group.state()) {
            case BLOCKED -> {
                checkState(!runnableQueue.contains(groupKey), "Group in BLOCKED state should not be in queue: %s", groupKey);
                checkState(!baselineWeights.contains(groupKey));
            }
            case RUNNABLE -> {
                checkState(runnableQueue.contains(groupKey), "Group in RUNNABLE state should be in queue: %s", groupKey);
                checkState(baselineWeights.contains(groupKey));
            }
            case RUNNING -> {
                checkState(!runnableQueue.contains(groupKey), "Group in RUNNING state should not be in queue: %s", groupKey);
                checkState(baselineWeights.contains(groupKey));
            }
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        builder.append("Baseline weight: %s\n".formatted(baselineWeight()));
        builder.append("\n");

        for (Map.Entry<G, SchedulingGroup<T>> entry : groups.entrySet()) {
            G group = entry.getKey();
            SchedulingGroup<T> info = entry.getValue();

            String prefix = "%s %s".formatted(
                    group == runnableQueue.peek() ? "=>" : " -",
                    group);

            String details = switch (entry.getValue().state()) {
                case BLOCKED -> "[BLOCKED, saved delta = %s]".formatted(info.weight());
                case RUNNING, RUNNABLE -> "[%s, weight = %s, baseline = %s]".formatted(info.state(), info.weight(), info.baselineWeight());
            };

            builder.append((prefix + " " + details).indent(4));
            builder.append(info.toString().indent(8));
        }

        return builder.toString();
    }
}
