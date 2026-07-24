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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.execution.executor.scheduler.State.BLOCKED;
import static io.trino.execution.executor.scheduler.State.RUNNABLE;
import static io.trino.execution.executor.scheduler.State.RUNNING;

/// A node in a CFS-style scheduling *tree* of arbitrary depth. An internal node schedules its
/// runnable children; a leaf wraps a scheduled handle `T`; the root is an internal node whose own
/// weight is never read by a parent. Modeled after the
/// [Completely Fair Scheduler](https://en.wikipedia.org/wiki/Completely_Fair_Scheduler): every
/// internal node orders its runnable children by accumulated weight (virtual runtime), exactly as
/// CFS orders runnable entities.
///
/// Each child sits in one of three states, tracked by its parent:
///
///   - **runnable**: a leaf that is ready to run and waiting to be dequeued, or an internal node
///     with at least one runnable descendant;
///   - **running**: a leaf that has been dequeued and is running, or an internal node all of whose
///     children are running;
///   - **blocked**: a leaf waiting on an external event, or an internal node with no children or
///     all of whose children are blocked. (An emptied group is therefore blocked — it stays out of
///     its parent's runnable set and baseline and cannot take a share from live siblings.)
///
/// A child starts blocked and moves between states as it is enqueued, dequeued, blocked and
/// finished. The same transitions govern a leaf (task) and an internal node (group):
///
/// ```
///                                                              block()
///     finish()                enqueue()                       enqueue()
///        ┌───┐   ┌──────────────────────────────────────────┐    ┌────┐
///        │   │   │                                          │    │    │
///        │   ▼   │                                          ▼    ▼    │
///      ┌─┴───────┴─┐   all blocked      finish()      ┌────────────┐  │
///      │           │◄──────────────O◄─────────────────┤            ├──┘
/// ────►│  BLOCKED  │               │                  │  RUNNABLE  │
///      │           │               │   ┌─────────────►│            │◄───┐
///      └───────────┘       not all │   │  enqueue()   └──────┬─────┘    │
///            ▲             blocked │   │                     │          │
///            │                     │   │          dequeue()  │          │
///            │ all blocked         ▼   │                     ▼          │
///            │                   ┌─────┴─────┐               │          │
///            │                   │           │◄──────────────O──────────┘
///            O◄──────────────────┤  RUNNING  │   queue empty     queue
///            │      block()      │           ├───┐            not empty
///            │                   └───────────┘   │
///            │                     ▲      ▲      │ finish()
///            └─────────────────────┘      └──────┘
///                not all blocked
/// ```
///
/// The three write operations ([#enqueue], [#block], [#dequeue]) are path operations from the root
/// to a leaf. Weight bookkeeping propagates along that path:
///
///   - A leaf remembers its own outstanding quantum (`uncommittedWeight`); an internal node folds
///     all its running descendants' quanta into its single `weight` scalar.
///   - [#dequeue] charges the quantum to *every* node on the path; [#enqueue] reconciles
///     `delta - quantum` at every node, so each level's subtree total advances by exactly the CPU
///     its subtree consumed — depth does not distort fairness at any level.
///   - Baseline freeze/thaw is applied at every level: a node that becomes fully blocked stores its
///     weight as an offset below the parent's baseline, and rebases to the current baseline when it
///     becomes runnable again.
@NotThreadSafe
final class SchedulingNode<T>
{
    private final boolean leaf;
    private final T handle; // leaf only

    private State state;
    private long weight;
    private long uncommittedWeight; // leaf only

    // internal nodes only
    private final Map<Object, SchedulingNode<T>> children = new HashMap<>();
    private final PriorityQueue<SchedulingNode<T>> runnable = new PriorityQueue<>();
    private final PriorityQueue<SchedulingNode<T>> baseline = new PriorityQueue<>();
    private final Set<SchedulingNode<T>> blocked = new HashSet<>();

    /// Creates a root node.
    public SchedulingNode()
    {
        this(false, null);
    }

    private SchedulingNode(boolean leaf, T handle)
    {
        this.leaf = leaf;
        this.handle = handle;
        this.state = BLOCKED;
    }

    // ------------------------------------------------------------------
    // Public (root-level) API
    // ------------------------------------------------------------------

    public void startGroup(List<Object> path)
    {
        SchedulingNode<T> parent = navigate(path.subList(0, path.size() - 1));
        Object key = path.getLast();
        checkArgument(!parent.children.containsKey(key), "Group already started: %s", path);

        SchedulingNode<T> group = new SchedulingNode<>(false, null);
        parent.children.put(key, group);
        parent.blocked.add(group); // empty group starts blocked
        parent.updateState();
    }

    public boolean containsGroup(List<Object> path)
    {
        return find(path) != null;
    }

    public State state(List<Object> path)
    {
        SchedulingNode<T> node = find(path);
        checkArgument(node != null, "Unknown group: %s", path);
        return node.state;
    }

    public Set<T> getTasks(List<Object> path)
    {
        SchedulingNode<T> node = find(path);
        checkArgument(node != null, "Unknown group: %s", path);
        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        node.collectLeaves(builder);
        return builder.build();
    }

    public Set<T> finishGroup(List<Object> path)
    {
        SchedulingNode<T> parent = navigate(path.subList(0, path.size() - 1));
        Object key = path.getLast();
        SchedulingNode<T> group = parent.children.remove(key);
        checkArgument(group != null, "Unknown group: %s", path);

        parent.runnable.removeIfPresent(group);
        parent.baseline.removeIfPresent(group);
        parent.blocked.remove(group);
        parent.updateState();

        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        group.collectLeaves(builder);
        return builder.build();
    }

    public Set<T> finishAll()
    {
        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        collectLeaves(builder);
        children.clear();
        runnable.clear();
        baseline.clear();
        blocked.clear();
        updateState();
        return builder.build();
    }

    public void enqueue(List<Object> path, long delta)
    {
        enqueueRecursive(path, 0, delta);
    }

    public void block(List<Object> path, long delta)
    {
        blockRecursive(path, 0, delta);
    }

    public void finish(List<Object> path)
    {
        finishRecursive(path, 0);
    }

    public T dequeue(long quantum)
    {
        if (runnable.isEmpty()) {
            return null;
        }
        return dequeueRecursive(quantum).handle;
    }

    public T peek()
    {
        SchedulingNode<T> node = this;
        while (!node.leaf) {
            node = node.runnable.peek();
            if (node == null) {
                return null;
            }
        }
        return node.handle;
    }

    public int getRunnableCount()
    {
        return runnableLeafCount();
    }

    // ------------------------------------------------------------------
    // Recursive operations
    // ------------------------------------------------------------------

    /// Enqueue (make runnable) the leaf reached by `path[index..]`, charging `delta` for the work
    /// its subtree just performed. Returns the leaf's reconciled quantum so ancestors can subtract
    /// it from their own accumulated weight.
    private long enqueueRecursive(List<Object> path, int index, long delta)
    {
        Object childKey = path.get(index);
        boolean childIsLeaf = index == path.size() - 1;

        SchedulingNode<T> child = children.get(childKey);
        boolean isNew = child == null;
        if (isNew) {
            checkArgument(childIsLeaf, "Group not started: %s", path.subList(0, index + 1));
            child = new SchedulingNode<>(true, cast(childKey));
            children.put(childKey, child);
        }
        State previousChildState = isNew ? BLOCKED : child.state;

        long quantum;
        if (childIsLeaf) {
            quantum = child.uncommittedWeight;
            child.weight += delta; // commit
            child.uncommittedWeight = 0;
            child.state = RUNNABLE;
        }
        else {
            quantum = child.enqueueRecursive(path, index + 1, delta);
        }

        // Each ancestor's subtree total advances by the net work (delta), i.e. it was charged the
        // quantum on dequeue and is reconciled here.
        weight += delta - quantum;

        if (previousChildState == BLOCKED) {
            // Rebase a newly-runnable child to the current baseline so it neither monopolizes
            // (large deficit) nor is starved (large surplus) after being blocked.
            child.weight += baselineWeight();
        }

        blocked.remove(child);
        runnable.addOrReplace(child, child.orderingWeight());
        baseline.addOrReplace(child, child.orderingWeight());
        updateState();

        return quantum;
    }

    private void blockRecursive(List<Object> path, int index, long delta)
    {
        Object childKey = path.get(index);
        boolean childIsLeaf = index == path.size() - 1;

        SchedulingNode<T> child = children.get(childKey);
        checkArgument(child != null, "Unknown %s: %s", childIsLeaf ? "task" : "group", path);
        State previousChildState = child.state;
        checkArgument(previousChildState == RUNNABLE || previousChildState == RUNNING, "Already blocked: %s", path);

        if (childIsLeaf) {
            weight += delta;
            child.weight += delta; // commit
            child.uncommittedWeight = 0;
            child.state = BLOCKED;
        }
        else {
            child.blockRecursive(path, index + 1, delta);
        }

        transitionChild(child, previousChildState);
        updateState();
    }

    private void finishRecursive(List<Object> path, int index)
    {
        Object childKey = path.get(index);
        boolean childIsLeaf = index == path.size() - 1;

        SchedulingNode<T> child = children.get(childKey);
        checkArgument(child != null, "Unknown %s: %s", childIsLeaf ? "task" : "group", path);

        if (childIsLeaf) {
            children.remove(childKey);
            blocked.remove(child);
            runnable.removeIfPresent(child);
            baseline.removeIfPresent(child);
            updateState();
        }
        else {
            State previousChildState = child.state;
            child.finishRecursive(path, index + 1);
            transitionChild(child, previousChildState);
            updateState();
        }
    }

    private SchedulingNode<T> dequeueRecursive(long quantum)
    {
        SchedulingNode<T> child = runnable.poll();
        verify(child != null);

        SchedulingNode<T> leaf;
        if (child.leaf) {
            child.uncommittedWeight = quantum;
            child.state = RUNNING;
            leaf = child;
        }
        else {
            leaf = child.dequeueRecursive(quantum);
        }

        weight += quantum;
        baseline.addOrReplace(child, child.orderingWeight());
        if (child.state == RUNNABLE) {
            runnable.add(child, child.orderingWeight());
        }
        updateState();

        return leaf;
    }

    /// Re-key `child` within this node after its state changed to `child.state` from
    /// `previousChildState`, applying the freeze on a fresh transition into BLOCKED.
    private void transitionChild(SchedulingNode<T> child, State previousChildState)
    {
        switch (child.state) {
            case RUNNABLE -> {
                blocked.remove(child);
                runnable.addOrReplace(child, child.orderingWeight());
                baseline.addOrReplace(child, child.orderingWeight());
            }
            case RUNNING -> {
                runnable.removeIfPresent(child);
                baseline.addOrReplace(child, child.orderingWeight());
            }
            case BLOCKED -> {
                if (previousChildState != BLOCKED) {
                    child.weight -= baselineWeight();
                    blocked.add(child);
                    runnable.removeIfPresent(child);
                    baseline.removeIfPresent(child);
                }
            }
        }
    }

    private void updateState()
    {
        if (leaf) {
            return;
        }
        if (children.isEmpty() || blocked.size() == children.size()) {
            state = BLOCKED;
        }
        else if (runnable.isEmpty()) {
            state = RUNNING;
        }
        else {
            state = RUNNABLE;
        }
    }

    private long orderingWeight()
    {
        return leaf ? weight + uncommittedWeight : weight;
    }

    private long baselineWeight()
    {
        return baseline.isEmpty() ? 0 : baseline.nextPriority();
    }

    private int runnableLeafCount()
    {
        if (leaf) {
            return 1;
        }
        int count = 0;
        for (SchedulingNode<T> child : runnable.values()) {
            count += child.runnableLeafCount();
        }
        return count;
    }

    private void collectLeaves(ImmutableSet.Builder<T> builder)
    {
        if (leaf) {
            builder.add(handle);
            return;
        }
        for (SchedulingNode<T> child : children.values()) {
            child.collectLeaves(builder);
        }
    }

    private SchedulingNode<T> navigate(List<Object> path)
    {
        SchedulingNode<T> node = this;
        for (Object key : path) {
            node = node.children.get(key);
            checkArgument(node != null, "Unknown group: %s", path);
        }
        return node;
    }

    private SchedulingNode<T> find(List<Object> path)
    {
        SchedulingNode<T> node = this;
        for (Object key : path) {
            node = node.children.get(key);
            if (node == null) {
                return null;
            }
        }
        return node;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        appendTo(builder, "root", 0);
        return builder.toString();
    }

    private void appendTo(StringBuilder builder, Object key, int depth)
    {
        String indent = "    ".repeat(depth);
        if (leaf) {
            builder.append("%s%s [%s, weight=%s, uncommitted=%s]%n".formatted(indent, key, state, weight, uncommittedWeight));
            return;
        }
        builder.append("%s%s [%s, weight=%s, baseline=%s]%n".formatted(indent, key, state, weight, baselineWeight()));
        for (Map.Entry<Object, SchedulingNode<T>> entry : children.entrySet()) {
            entry.getValue().appendTo(builder, entry.getKey(), depth + 1);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object key)
    {
        return (T) key;
    }
}
