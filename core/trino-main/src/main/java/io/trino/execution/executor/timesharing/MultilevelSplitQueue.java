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
package io.trino.execution.executor.timesharing;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.stats.CounterStat;
import io.trino.execution.TaskManagerConfig;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class MultilevelSplitQueue
{
    static final int[] LEVEL_THRESHOLD_SECONDS = {0, 1, 10, 60, 300};
    static final long LEVEL_CONTRIBUTION_CAP = SECONDS.toNanos(30);

    @GuardedBy("lock")
    private final PriorityQueue<PrioritizedSplitRunner>[] levelWaitingSplits;

    private final AtomicLong[] levelScheduledTime;

    private final AtomicLong[] levelMinPriority;
    private final CounterStat[] selectedLevelCounters;

    private final double[] levelWeights = new double[LEVEL_THRESHOLD_SECONDS.length];

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private final double levelTimeMultiplier;

    @Inject
    public MultilevelSplitQueue(TaskManagerConfig taskManagerConfig)
    {
        this(taskManagerConfig.getLevelTimeMultiplier().doubleValue());
    }

    public MultilevelSplitQueue(double levelTimeMultiplier)
    {
        this.levelScheduledTime = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
        this.levelMinPriority = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
        this.levelWaitingSplits = new PriorityQueue[LEVEL_THRESHOLD_SECONDS.length];
        this.selectedLevelCounters = new CounterStat[LEVEL_THRESHOLD_SECONDS.length];

        for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
            levelScheduledTime[level] = new AtomicLong();
            levelMinPriority[level] = new AtomicLong(-1);
            levelWaitingSplits[level] = new PriorityQueue<>();
            selectedLevelCounters[level] = new CounterStat();
            levelWeights[level] = Math.pow(levelTimeMultiplier, LEVEL_THRESHOLD_SECONDS.length - 1 - level);
        }

        this.levelTimeMultiplier = levelTimeMultiplier;
    }

    private void addLevelTime(int level, long nanos)
    {
        levelScheduledTime[level].addAndGet(nanos);
    }

    /**
     * During periods of time when a level has no waiting splits, it will not accumulate
     * scheduled time and will fall behind relative to other levels.
     * <p>
     * This can cause temporary starvation for other levels when splits do reach the
     * previously-empty level.
     * <p>
     * To prevent this we set the scheduled time for levels which were empty to the expected
     * scheduled time.
     */
    public void offer(PrioritizedSplitRunner split)
    {
        checkArgument(split != null, "split is null");

        split.setReady();
        int level = split.getPriority().getLevel();
        lock.lock();
        try {
            if (levelWaitingSplits[level].isEmpty()) {
                // Accesses to levelScheduledTime are not synchronized, so we have a data race
                // here - our level time math will be off. However, the staleness is bounded by
                // the fact that only running splits that complete during this computation
                // can update the level time. Therefore, this is benign.
                long level0Time = getLevel0TargetTime();
                long levelExpectedTime = (long) (level0Time / levelWeights[levelWeights.length - 1 - level]);
                long delta = levelExpectedTime - levelScheduledTime[level].get();
                levelScheduledTime[level].addAndGet(delta);
            }

            levelWaitingSplits[level].offer(split);
            notEmpty.signal();
        }
        finally {
            lock.unlock();
        }
    }

    public PrioritizedSplitRunner take()
            throws InterruptedException
    {
        while (true) {
            lock.lockInterruptibly();
            try {
                PrioritizedSplitRunner result;
                while ((result = pollSplit()) == null) {
                    notEmpty.await();
                }

                if (result.updateLevelPriority()) {
                    offer(result);
                    continue;
                }

                int selectedLevel = result.getPriority().getLevel();
                levelMinPriority[selectedLevel].set(result.getPriority().getLevelPriority());
                selectedLevelCounters[selectedLevel].update(1);

                return result;
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * Trino attempts to give each level a target amount of scheduled time, which is configurable
     * using levelTimeMultiplier.
     * <p>
     * This function selects the level that has the lowest ratio of actual to the target time
     * with the objective of minimizing deviation from the target scheduled time. From this level,
     * we pick the split with the lowest priority.
     */
    @GuardedBy("lock")
    private PrioritizedSplitRunner pollSplit()
    {
        double minScheduledTimeWithoutWeight = Double.MAX_VALUE;
        int selectedLevel = -1;
        for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
            if (!levelWaitingSplits[level].isEmpty()) {
                long levelTime = levelScheduledTime[level].get();
                double scheduledTimeWithoutWeight = levelTime / levelWeights[level];
                if (selectedLevel == -1 || scheduledTimeWithoutWeight < minScheduledTimeWithoutWeight) {
                    minScheduledTimeWithoutWeight = scheduledTimeWithoutWeight;
                    selectedLevel = level;
                }
            }
        }

        if (selectedLevel == -1) {
            return null;
        }

        PrioritizedSplitRunner result = levelWaitingSplits[selectedLevel].poll();
        checkState(result != null, "pollSplit cannot return null");

        return result;
    }

    @GuardedBy("lock")
    private long getLevel0TargetTime()
    {
        long level0TargetTime = levelScheduledTime[0].get();
        double currentMultiplier = levelTimeMultiplier;

        for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
            currentMultiplier /= levelTimeMultiplier;
            long levelTime = levelScheduledTime[level].get();
            level0TargetTime = Math.max(level0TargetTime, (long) (levelTime / currentMultiplier));
        }

        return level0TargetTime;
    }

    /**
     * Trino 'charges' the quanta run time to the task <i>and</i> the level it belongs to in
     * an effort to maintain the target thread utilization ratios between levels and to
     * maintain fairness within a level.
     * <p>
     * Consider an example split where a read hung for several minutes. This is either a bug
     * or a failing dependency. In either case we do not want to charge the task too much,
     * and we especially do not want to charge the level too much - i.e. cause other queries
     * in this level to starve.
     *
     * @return the new priority for the task
     */
    public Priority updatePriority(Priority oldPriority, long quantaNanos, long scheduledNanos)
    {
        int oldLevel = oldPriority.getLevel();
        int newLevel = computeLevel(scheduledNanos);

        long levelContribution = Math.min(quantaNanos, LEVEL_CONTRIBUTION_CAP);

        if (oldLevel == newLevel) {
            addLevelTime(oldLevel, levelContribution);
            return new Priority(oldLevel, oldPriority.getLevelPriority() + quantaNanos);
        }

        long remainingLevelContribution = levelContribution;
        long remainingTaskTime = quantaNanos;

        // a task normally slowly accrues scheduled time in a level and then moves to the next, but
        // if the split had a particularly long quanta, accrue time to each level as if it had run
        // in that level up to the level limit.
        for (int currentLevel = oldLevel; currentLevel < newLevel; currentLevel++) {
            long timeAccruedToLevel = Math.min(SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[currentLevel + 1] - LEVEL_THRESHOLD_SECONDS[currentLevel]), remainingLevelContribution);
            addLevelTime(currentLevel, timeAccruedToLevel);
            remainingLevelContribution -= timeAccruedToLevel;
            remainingTaskTime -= timeAccruedToLevel;
        }

        addLevelTime(newLevel, remainingLevelContribution);
        long newLevelMinPriority = getLevelMinPriority(newLevel, scheduledNanos);
        return new Priority(newLevel, newLevelMinPriority + remainingTaskTime);
    }

    public void remove(PrioritizedSplitRunner split)
    {
        checkArgument(split != null, "split is null");
        lock.lock();
        try {
            for (PriorityQueue<PrioritizedSplitRunner> level : levelWaitingSplits) {
                level.remove(split);
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void removeAll(Collection<PrioritizedSplitRunner> splits)
    {
        lock.lock();
        try {
            for (PriorityQueue<PrioritizedSplitRunner> level : levelWaitingSplits) {
                level.removeAll(splits);
            }
        }
        finally {
            lock.unlock();
        }
    }

    public long getLevelMinPriority(int level, long taskThreadUsageNanos)
    {
        levelMinPriority[level].compareAndSet(-1, taskThreadUsageNanos);
        return levelMinPriority[level].get();
    }

    public int size()
    {
        lock.lock();
        try {
            int total = 0;
            for (PriorityQueue<PrioritizedSplitRunner> level : levelWaitingSplits) {
                total += level.size();
            }
            return total;
        }
        finally {
            lock.unlock();
        }
    }

    public static int computeLevel(long threadUsageNanos)
    {
        long seconds = NANOSECONDS.toSeconds(threadUsageNanos);
        for (int level = 0; level < (LEVEL_THRESHOLD_SECONDS.length - 1); level++) {
            if (seconds < LEVEL_THRESHOLD_SECONDS[level + 1]) {
                return level;
            }
        }

        return LEVEL_THRESHOLD_SECONDS.length - 1;
    }

    @VisibleForTesting
    long getLevelScheduledTime(int level)
    {
        return levelScheduledTime[level].longValue();
    }

    @Managed
    public long getLevel0Time()
    {
        return getLevelScheduledTime(0);
    }

    @Managed
    public long getLevel1Time()
    {
        return getLevelScheduledTime(1);
    }

    @Managed
    public long getLevel2Time()
    {
        return getLevelScheduledTime(2);
    }

    @Managed
    public long getLevel3Time()
    {
        return getLevelScheduledTime(3);
    }

    @Managed
    public long getLevel4Time()
    {
        return getLevelScheduledTime(4);
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel0()
    {
        return selectedLevelCounters[0];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel1()
    {
        return selectedLevelCounters[1];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel2()
    {
        return selectedLevelCounters[2];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel3()
    {
        return selectedLevelCounters[3];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel4()
    {
        return selectedLevelCounters[4];
    }
}
