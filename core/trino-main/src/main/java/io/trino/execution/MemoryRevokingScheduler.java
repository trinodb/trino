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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.FeaturesConfig;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.MemoryPool;
import io.trino.memory.MemoryPoolListener;
import io.trino.memory.TraversingQueryContextVisitor;
import io.trino.memory.VoidTraversingQueryContextVisitor;
import io.trino.operator.OperatorContext;
import io.trino.operator.PipelineContext;
import io.trino.operator.TaskContext;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MemoryRevokingScheduler
{
    private static final Logger log = Logger.get(MemoryRevokingScheduler.class);

    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(SqlTask::getTaskCreatedTime);
    private final MemoryPool memoryPool;
    private final Supplier<? extends Collection<SqlTask>> currentTasksSupplier;
    private final ScheduledExecutorService taskManagementExecutor;
    private final double memoryRevokingThreshold;
    private final double memoryRevokingTarget;

    private final MemoryPoolListener memoryPoolListener = MemoryPoolListener.onMemoryReserved(this::onMemoryReserved);

    @Nullable
    private ScheduledFuture<?> scheduledFuture;

    private final AtomicBoolean checkPending = new AtomicBoolean();

    @Inject
    public MemoryRevokingScheduler(
            LocalMemoryManager localMemoryManager,
            SqlTaskManager sqlTaskManager,
            TaskManagementExecutor taskManagementExecutor,
            FeaturesConfig config)
    {
        this(
                localMemoryManager.getMemoryPool(),
                sqlTaskManager::getAllTasks,
                taskManagementExecutor.getExecutor(),
                config.getMemoryRevokingThreshold(),
                config.getMemoryRevokingTarget());
    }

    @VisibleForTesting
    MemoryRevokingScheduler(
            MemoryPool memoryPool,
            Supplier<? extends Collection<SqlTask>> currentTasksSupplier,
            ScheduledExecutorService taskManagementExecutor,
            double memoryRevokingThreshold,
            double memoryRevokingTarget)
    {
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.currentTasksSupplier = requireNonNull(currentTasksSupplier, "currentTasksSupplier is null");
        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor is null");
        this.memoryRevokingThreshold = checkFraction(memoryRevokingThreshold, "memoryRevokingThreshold");
        this.memoryRevokingTarget = checkFraction(memoryRevokingTarget, "memoryRevokingTarget");
        checkArgument(
                memoryRevokingTarget <= memoryRevokingThreshold,
                "memoryRevokingTarget should be less than or equal memoryRevokingThreshold, but got %s and %s respectively",
                memoryRevokingTarget, memoryRevokingThreshold);
    }

    private static double checkFraction(double value, String valueName)
    {
        requireNonNull(valueName, "valueName is null");
        checkArgument(0 <= value && value <= 1, "%s should be within [0, 1] range, got %s", valueName, value);
        return value;
    }

    @PostConstruct
    public void start()
    {
        registerPeriodicCheck();
        registerPoolListeners();
    }

    private void registerPeriodicCheck()
    {
        this.scheduledFuture = taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                requestMemoryRevokingIfNeeded();
            }
            catch (Throwable e) {
                log.error(e, "Error requesting system memory revoking");
            }
        }, 1, 1, SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }

        memoryPool.removeListener(memoryPoolListener);
    }

    @VisibleForTesting
    void registerPoolListeners()
    {
        memoryPool.addListener(memoryPoolListener);
    }

    private void onMemoryReserved(MemoryPool memoryPool)
    {
        try {
            if (!memoryRevokingNeeded(memoryPool)) {
                return;
            }

            if (checkPending.compareAndSet(false, true)) {
                log.debug("Scheduling check for %s", memoryPool);
                scheduleRevoking();
            }
        }
        catch (Throwable e) {
            log.error(e, "Error when acting on memory pool reservation");
        }
    }

    @VisibleForTesting
    void requestMemoryRevokingIfNeeded()
    {
        if (checkPending.compareAndSet(false, true)) {
            runMemoryRevoking();
        }
    }

    private void scheduleRevoking()
    {
        taskManagementExecutor.execute(() -> {
            try {
                runMemoryRevoking();
            }
            catch (Throwable e) {
                log.error(e, "Error requesting memory revoking");
            }
        });
    }

    private synchronized void runMemoryRevoking()
    {
        if (checkPending.getAndSet(false)) {
            if (!memoryRevokingNeeded(memoryPool)) {
                return;
            }
            requestMemoryRevoking(memoryPool, requireNonNull(currentTasksSupplier.get()));
        }
    }

    private void requestMemoryRevoking(MemoryPool memoryPool, Collection<SqlTask> allTasks)
    {
        long remainingBytesToRevoke = (long) (-memoryPool.getFreeBytes() + (memoryPool.getMaxBytes() * (1.0 - memoryRevokingTarget)));
        List<SqlTask> runningTasksInPool = findRunningTasksInMemoryPool(allTasks, memoryPool);
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(runningTasksInPool, remainingBytesToRevoke);
        if (remainingBytesToRevoke > 0) {
            requestRevoking(runningTasksInPool, remainingBytesToRevoke);
        }
    }

    private boolean memoryRevokingNeeded(MemoryPool memoryPool)
    {
        return memoryPool.getReservedRevocableBytes() > 0
                && memoryPool.getFreeBytes() <= memoryPool.getMaxBytes() * (1.0 - memoryRevokingThreshold);
    }

    private long getMemoryAlreadyBeingRevoked(List<SqlTask> sqlTasks, long targetRevokingLimit)
    {
        TraversingQueryContextVisitor<Void, Long> visitor = new TraversingQueryContextVisitor<>()
        {
            @Override
            public Long visitOperatorContext(OperatorContext operatorContext, Void context)
            {
                if (operatorContext.isMemoryRevokingRequested()) {
                    return operatorContext.getReservedRevocableBytes();
                }
                return 0L;
            }

            @Override
            public Long mergeResults(List<Long> childrenResults)
            {
                return childrenResults.stream()
                        .mapToLong(i -> i).sum();
            }
        };

        long currentRevoking = 0;
        for (SqlTask task : sqlTasks) {
            Optional<TaskContext> taskContext = task.getTaskContext();
            if (taskContext.isPresent()) {
                currentRevoking += taskContext.get().accept(visitor, null);
                if (currentRevoking >= targetRevokingLimit) {
                    // Return early, target value exceeded and revoking will not occur
                    return currentRevoking;
                }
            }
        }
        return currentRevoking;
    }

    private void requestRevoking(List<SqlTask> sqlTasks, long remainingBytesToRevoke)
    {
        VoidTraversingQueryContextVisitor<AtomicLong> visitor = new VoidTraversingQueryContextVisitor<>()
        {
            @Override
            public Void visitPipelineContext(PipelineContext pipelineContext, AtomicLong remainingBytesToRevoke)
            {
                if (remainingBytesToRevoke.get() <= 0) {
                    // exit immediately if no work needs to be done
                    return null;
                }
                return super.visitPipelineContext(pipelineContext, remainingBytesToRevoke);
            }

            @Override
            public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
            {
                if (remainingBytesToRevoke.get() > 0) {
                    long revokedBytes = operatorContext.requestMemoryRevoking();
                    if (revokedBytes > 0) {
                        remainingBytesToRevoke.addAndGet(-revokedBytes);
                        log.debug("requested revoking %s; remaining %s", revokedBytes, remainingBytesToRevoke.get());
                    }
                }
                return null;
            }
        };

        AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
        for (SqlTask task : sqlTasks) {
            Optional<TaskContext> taskContext = task.getTaskContext();
            if (taskContext.isPresent()) {
                taskContext.get().accept(visitor, remainingBytesToRevokeAtomic);
                if (remainingBytesToRevokeAtomic.get() <= 0) {
                    // No further revoking required
                    return;
                }
            }
        }
    }

    private static List<SqlTask> findRunningTasksInMemoryPool(Collection<SqlTask> allCurrentTasks, MemoryPool memoryPool)
    {
        return allCurrentTasks.stream()
                .filter(task -> task.getTaskState() == TaskState.RUNNING && task.getQueryContext().getMemoryPool() == memoryPool)
                .sorted(ORDER_BY_CREATE_TIME)
                .collect(toImmutableList());
    }
}
