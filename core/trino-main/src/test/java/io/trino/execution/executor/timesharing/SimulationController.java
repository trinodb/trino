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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import io.airlift.log.Logger;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.executor.timesharing.SimulationTask.IntermediateTask;
import io.trino.execution.executor.timesharing.SimulationTask.LeafTask;
import io.trino.execution.executor.timesharing.SplitGenerators.SplitGenerator;

import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static io.trino.execution.executor.timesharing.SimulationController.TaskSpecification.Type.LEAF;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class SimulationController
{
    private static final Logger log = Logger.get(SimulationController.class);

    private static final int DEFAULT_MIN_SPLITS_PER_TASK = 3;

    private final TimeSharingTaskExecutor taskExecutor;
    private final BiConsumer<SimulationController, TimeSharingTaskExecutor> callback;

    private final ExecutorService controllerExecutor = newSingleThreadExecutor();

    private final Map<TaskSpecification, Boolean> specificationEnabled = new ConcurrentHashMap<>();
    private final ListMultimap<TaskSpecification, SimulationTask> runningTasks = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());

    private final ListMultimap<TaskSpecification, SimulationTask> completedTasks = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
    private final AtomicBoolean clearPendingQueue = new AtomicBoolean();

    private final AtomicBoolean stopped = new AtomicBoolean();

    public SimulationController(TimeSharingTaskExecutor taskExecutor, BiConsumer<SimulationController, TimeSharingTaskExecutor> callback)
    {
        this.taskExecutor = taskExecutor;
        this.callback = callback;
    }

    public synchronized void addTaskSpecification(TaskSpecification spec)
    {
        specificationEnabled.put(spec, false);
    }

    public synchronized void clearPendingQueue()
    {
        log.info("Clearing pending queue..");
        clearPendingQueue.set(true);
    }

    public synchronized void stop()
    {
        stopped.set(true);
        controllerExecutor.shutdownNow();
        taskExecutor.stop();
    }

    public synchronized void enableSpecification(TaskSpecification specification)
    {
        specificationEnabled.replace(specification, false, true);
        startSpec(specification);
    }

    public synchronized void disableSpecification(TaskSpecification specification)
    {
        if (specificationEnabled.replace(specification, true, false) && callback != null) {
            runCallback();
        }
    }

    public synchronized void runCallback()
    {
        callback.accept(this, taskExecutor);
    }

    public void run()
    {
        controllerExecutor.submit(() -> {
            while (!stopped.get()) {
                replaceCompletedTasks();
                scheduleSplitsForRunningTasks();

                try {
                    MILLISECONDS.sleep(500);
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    private synchronized void scheduleSplitsForRunningTasks()
    {
        if (clearPendingQueue.get()) {
            if (taskExecutor.getWaitingSplits() > (taskExecutor.getIntermediateSplits() - taskExecutor.getBlockedSplits())) {
                return;
            }

            log.info("Clearing pending queue.");
            clearPendingQueue.set(false);
        }

        for (TaskSpecification specification : specificationEnabled.keySet()) {
            if (!specificationEnabled.get(specification)) {
                continue;
            }

            for (SimulationTask task : runningTasks.get(specification)) {
                if (specification.getType() == LEAF) {
                    int remainingSplits = specification.getNumSplitsPerTask() - (task.getRunningSplits().size() + task.getCompletedSplits().size());
                    int candidateSplits = DEFAULT_MIN_SPLITS_PER_TASK - task.getRunningSplits().size();
                    for (int i = 0; i < Math.min(remainingSplits, candidateSplits); i++) {
                        task.schedule(taskExecutor, 1);
                    }
                }
                else {
                    int remainingSplits = specification.getNumSplitsPerTask() - (task.getRunningSplits().size() + task.getCompletedSplits().size());
                    task.schedule(taskExecutor, remainingSplits);
                }
            }
        }
    }

    private synchronized void replaceCompletedTasks()
    {
        boolean moved;
        do {
            moved = false;

            for (TaskSpecification specification : specificationEnabled.keySet()) {
                if (specification.getTotalTasks().isPresent() &&
                        specificationEnabled.get(specification) &&
                        specification.getTotalTasks().getAsInt() <= completedTasks.get(specification).size() + runningTasks.get(specification).size()) {
                    log.info("\n%s disabled for reaching target count %s\n", specification.getName(), specification.getTotalTasks());
                    disableSpecification(specification);
                    continue;
                }
                for (SimulationTask task : runningTasks.get(specification)) {
                    if (task.getCompletedSplits().size() >= specification.getNumSplitsPerTask()) {
                        completedTasks.put(specification, task);
                        runningTasks.remove(specification, task);
                        taskExecutor.removeTask(task.getTaskHandle());

                        if (!specificationEnabled.get(specification)) {
                            continue;
                        }

                        createTask(specification);
                        moved = true;
                        break;
                    }
                }
            }
        }
        while (moved);
    }

    private void createTask(TaskSpecification specification)
    {
        if (specification.getType() == LEAF) {
            runningTasks.put(specification, new LeafTask(
                    taskExecutor,
                    specification,
                    new TaskId(new StageId(specification.getName(), 0), runningTasks.get(specification).size() + completedTasks.get(specification).size(), 0)));
        }
        else {
            runningTasks.put(specification, new IntermediateTask(
                    taskExecutor,
                    specification,
                    new TaskId(new StageId(specification.getName(), 0), runningTasks.get(specification).size() + completedTasks.get(specification).size(), 0)));
        }
    }

    public Map<TaskSpecification, Boolean> getSpecificationEnabled()
    {
        return specificationEnabled;
    }

    public ListMultimap<TaskSpecification, SimulationTask> getRunningTasks()
    {
        return runningTasks;
    }

    public ListMultimap<TaskSpecification, SimulationTask> getCompletedTasks()
    {
        return completedTasks;
    }

    private void startSpec(TaskSpecification specification)
    {
        if (!specificationEnabled.get(specification)) {
            return;
        }
        for (int i = 0; i < specification.getNumConcurrentTasks(); i++) {
            createTask(specification);
        }
    }

    public static class TaskSpecification
    {
        enum Type
        {
            LEAF,
            INTERMEDIATE
        }

        private final Type type;
        private final String name;
        private final OptionalInt totalTasks;
        private final int numConcurrentTasks;
        private final int numSplitsPerTask;
        private final SplitGenerator splitGenerator;

        TaskSpecification(Type type, String name, OptionalInt totalTasks, int numConcurrentTasks, int numSplitsPerTask, SplitGenerator splitGenerator)
        {
            this.type = type;
            this.name = name;
            this.totalTasks = totalTasks;
            this.numConcurrentTasks = numConcurrentTasks;
            this.numSplitsPerTask = numSplitsPerTask;
            this.splitGenerator = splitGenerator;
        }

        Type getType()
        {
            return type;
        }

        String getName()
        {
            return name;
        }

        int getNumConcurrentTasks()
        {
            return numConcurrentTasks;
        }

        int getNumSplitsPerTask()
        {
            return numSplitsPerTask;
        }

        OptionalInt getTotalTasks()
        {
            return totalTasks;
        }

        SplitSpecification nextSpecification()
        {
            return splitGenerator.next();
        }
    }
}
