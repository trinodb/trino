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
package io.trino.execution.executor.dedicated;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.CpuTimer;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.executor.scheduler.Schedulable;
import io.trino.execution.executor.scheduler.SchedulerContext;
import io.trino.tracing.TrinoAttributes;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class SplitProcessor
        implements Schedulable
{
    private static final Logger LOG = Logger.get(SplitProcessor.class);

    private static final Duration SPLIT_RUN_QUANTA = new Duration(1, TimeUnit.SECONDS);

    private final TaskId taskId;
    private final int splitId;
    private final SplitRunner split;
    private final Tracer tracer;

    public SplitProcessor(TaskId taskId, int splitId, SplitRunner split, Tracer tracer)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.splitId = splitId;
        this.split = requireNonNull(split, "split is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @Override
    public void run(SchedulerContext context)
    {
        Span splitSpan = tracer.spanBuilder("split")
                .setParent(Context.current().with(split.getPipelineSpan()))
                .setAttribute(TrinoAttributes.QUERY_ID, taskId.getQueryId().toString())
                .setAttribute(TrinoAttributes.STAGE_ID, taskId.getStageId().toString())
                .setAttribute(TrinoAttributes.TASK_ID, taskId.toString())
                .setAttribute(TrinoAttributes.PIPELINE_ID, taskId.getStageId() + "-" + split.getPipelineId())
                .setAttribute(TrinoAttributes.SPLIT_ID, taskId + "-" + splitId)
                .startSpan();

        Span processSpan = newSpan(splitSpan, null);

        CpuTimer timer = new CpuTimer(Ticker.systemTicker(), false);
        long previousCpuNanos = 0;
        long previousScheduledNanos = 0;
        try (SetThreadName ignored = new SetThreadName("SplitRunner-%s-%s", taskId, splitId)) {
            while (!split.isFinished()) {
                ListenableFuture<Void> blocked = split.processFor(SPLIT_RUN_QUANTA);
                CpuTimer.CpuDuration elapsed = timer.elapsedTime();

                long scheduledNanos = elapsed.getWall().roundTo(NANOSECONDS);
                processSpan.setAttribute(TrinoAttributes.SPLIT_SCHEDULED_TIME_NANOS, scheduledNanos - previousScheduledNanos);
                previousScheduledNanos = scheduledNanos;

                long cpuNanos = elapsed.getCpu().roundTo(NANOSECONDS);
                processSpan.setAttribute(TrinoAttributes.SPLIT_CPU_TIME_NANOS, cpuNanos - previousCpuNanos);
                previousCpuNanos = cpuNanos;

                if (!split.isFinished()) {
                    if (blocked.isDone()) {
                        processSpan.addEvent("yield");
                        processSpan.end();
                        if (!context.maybeYield()) {
                            processSpan = null;
                            return;
                        }
                    }
                    else {
                        processSpan.addEvent("blocked");
                        processSpan.end();
                        if (!context.block(blocked)) {
                            processSpan = null;
                            return;
                        }
                    }
                    processSpan = newSpan(splitSpan, processSpan);
                }
            }
        }
        catch (Exception e) {
            LOG.error(e);
        }
        finally {
            if (processSpan != null) {
                processSpan.end();
            }

            splitSpan.setAttribute(TrinoAttributes.SPLIT_CPU_TIME_NANOS, timer.elapsedTime().getCpu().roundTo(NANOSECONDS));
            splitSpan.setAttribute(TrinoAttributes.SPLIT_SCHEDULED_TIME_NANOS, context.getScheduledNanos());
            splitSpan.setAttribute(TrinoAttributes.SPLIT_BLOCK_TIME_NANOS, context.getBlockedNanos());
            splitSpan.setAttribute(TrinoAttributes.SPLIT_WAIT_TIME_NANOS, context.getWaitNanos());
            splitSpan.setAttribute(TrinoAttributes.SPLIT_START_TIME_NANOS, context.getStartNanos());
            splitSpan.end();
        }
    }

    private Span newSpan(Span parent, Span previous)
    {
        SpanBuilder builder = tracer.spanBuilder("process")
                .setParent(Context.current().with(parent));

        if (previous != null) {
            builder.addLink(previous.getSpanContext());
        }

        return builder.startSpan();
    }
}
