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
package io.trino.operator;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.operator.WorkProcessor.Transformation;
import io.trino.operator.WorkProcessor.TransformationState;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public final class WorkProcessorUtils
{
    private WorkProcessorUtils() {}

    static <T> Iterator<T> iteratorFrom(WorkProcessor<T> processor)
    {
        requireNonNull(processor, "processor is null");
        return new AbstractIterator<>()
        {
            final Iterator<Optional<T>> yieldingIterator = yieldingIteratorFrom(processor);

            @Override
            protected T computeNext()
            {
                if (!yieldingIterator.hasNext()) {
                    return endOfData();
                }

                return yieldingIterator.next()
                        .orElseThrow(() -> new IllegalStateException("Cannot iterate over yielding WorkProcessor"));
            }
        };
    }

    static <T> Iterator<Optional<T>> yieldingIteratorFrom(WorkProcessor<T> processor)
    {
        return new YieldingIterator<>(processor);
    }

    private static class YieldingIterator<T>
            extends AbstractIterator<Optional<T>>
    {
        @Nullable
        WorkProcessor<T> processor;

        YieldingIterator(WorkProcessor<T> processor)
        {
            this.processor = requireNonNull(processor, "processorParameter is null");
        }

        @Override
        protected Optional<T> computeNext()
        {
            if (processor.process()) {
                if (processor.isFinished()) {
                    processor = null;
                    return endOfData();
                }

                return Optional.of(processor.getResult());
            }

            if (processor.isBlocked()) {
                throw new IllegalStateException("Cannot iterate over blocking WorkProcessor");
            }

            // yielded
            return Optional.empty();
        }
    }

    static <T> WorkProcessor<T> fromIterator(Iterator<T> iterator)
    {
        requireNonNull(iterator, "iterator is null");
        return create(() -> {
            if (!iterator.hasNext()) {
                return ProcessState.finished();
            }

            return ProcessState.ofResult(iterator.next());
        });
    }

    static <T> WorkProcessor<T> mergeSorted(Iterable<WorkProcessor<T>> processorIterable, Comparator<T> comparator)
    {
        requireNonNull(comparator, "comparator is null");
        Iterator<WorkProcessor<T>> processorIterator = requireNonNull(processorIterable, "processorIterable is null").iterator();
        checkArgument(processorIterator.hasNext(), "There must be at least one base processor");
        PriorityQueue<ElementAndProcessor<T>> queue = new PriorityQueue<>(2, comparing(ElementAndProcessor::getElement, comparator));

        return create(new WorkProcessor.Process<>()
        {
            WorkProcessor<T> processor = requireNonNull(processorIterator.next());

            @Override
            public ProcessState<T> process()
            {
                while (true) {
                    if (processor.process()) {
                        if (!processor.isFinished()) {
                            queue.add(new ElementAndProcessor<>(processor.getResult(), processor));
                        }
                    }
                    else if (processor.isBlocked()) {
                        return ProcessState.blocked(processor.getBlockedFuture());
                    }
                    else {
                        return ProcessState.yielded();
                    }

                    if (processorIterator.hasNext()) {
                        processor = requireNonNull(processorIterator.next());
                        continue;
                    }

                    if (queue.isEmpty()) {
                        return ProcessState.finished();
                    }

                    ElementAndProcessor<T> elementAndProcessor = queue.poll();
                    processor = elementAndProcessor.getProcessor();
                    return ProcessState.ofResult(elementAndProcessor.getElement());
                }
            }
        });
    }

    static <T> WorkProcessor<T> yielding(WorkProcessor<T> processor, BooleanSupplier yieldSignal)
    {
        return WorkProcessor.create(new YieldingProcess<>(processor, yieldSignal));
    }

    private static class YieldingProcess<T>
            implements WorkProcessor.Process<T>
    {
        final WorkProcessor<T> processor;
        final BooleanSupplier yieldSignal;
        boolean lastProcessYielded;

        YieldingProcess(WorkProcessor<T> processor, BooleanSupplier yieldSignal)
        {
            this.processor = requireNonNull(processor, "processor is null");
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
        }

        @Override
        public ProcessState<T> process()
        {
            if (!lastProcessYielded && yieldSignal.getAsBoolean()) {
                lastProcessYielded = true;
                return ProcessState.yielded();
            }
            lastProcessYielded = false;

            return getNextState(processor);
        }
    }

    static <T> WorkProcessor<T> processEntryMonitor(WorkProcessor<T> processor, Runnable monitor)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(monitor, "monitor is null");
        return WorkProcessor.create(() -> {
            monitor.run();
            return getNextState(processor);
        });
    }

    static <T> WorkProcessor<T> processStateMonitor(WorkProcessor<T> processor, Consumer<ProcessState<T>> monitor)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(monitor, "monitor is null");
        return WorkProcessor.create(() -> {
            ProcessState<T> state = getNextState(processor);
            monitor.accept(state);
            return state;
        });
    }

    static <T> WorkProcessor<T> finishWhen(WorkProcessor<T> processor, BooleanSupplier finishSignal)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(finishSignal, "finishSignal is null");
        return WorkProcessor.create(() -> {
            if (finishSignal.getAsBoolean()) {
                return ProcessState.finished();
            }

            return getNextState(processor);
        });
    }

    private static <T> ProcessState<T> getNextState(WorkProcessor<T> processor)
    {
        if (processor.process()) {
            if (processor.isFinished()) {
                return ProcessState.finished();
            }

            return ProcessState.ofResult(processor.getResult());
        }

        if (processor.isBlocked()) {
            return ProcessState.blocked(processor.getBlockedFuture());
        }

        return ProcessState.yielded();
    }

    static <T, R> WorkProcessor<R> flatMap(WorkProcessor<T> processor, Function<T, WorkProcessor<R>> mapper)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(mapper, "mapper is null");
        return processor.flatTransform(element -> {
            if (element == null) {
                return TransformationState.finished();
            }

            return TransformationState.ofResult(mapper.apply(element));
        });
    }

    static <T, R> WorkProcessor<R> map(WorkProcessor<T> processor, Function<T, R> mapper)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(mapper, "mapper is null");
        return processor.transform(element -> {
            if (element == null) {
                return TransformationState.finished();
            }

            return TransformationState.ofResult(mapper.apply(element));
        });
    }

    static <T, R> WorkProcessor<R> flatTransform(WorkProcessor<T> processor, Transformation<T, WorkProcessor<R>> transformation)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(transformation, "transformation is null");
        return processor.transform(transformation).transformProcessor(WorkProcessorUtils::flatten);
    }

    static <T> WorkProcessor<T> flatten(WorkProcessor<WorkProcessor<T>> processor)
    {
        requireNonNull(processor, "processor is null");
        return processor.transform(nestedProcessor -> {
            if (nestedProcessor == null) {
                return TransformationState.finished();
            }

            if (nestedProcessor.process()) {
                if (nestedProcessor.isFinished()) {
                    return TransformationState.needsMoreData();
                }

                return TransformationState.ofResult(nestedProcessor.getResult(), false);
            }

            if (nestedProcessor.isBlocked()) {
                return TransformationState.blocked(nestedProcessor.getBlockedFuture());
            }

            return TransformationState.yielded();
        });
    }

    static <T, R> WorkProcessor<R> transform(WorkProcessor<T> processor, Transformation<T, R> transformation)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(transformation, "transformation is null");
        return create(new WorkProcessor.Process<>()
        {
            T element;

            @Override
            public ProcessState<R> process()
            {
                while (true) {
                    if (element == null && !processor.isFinished()) {
                        if (processor.process()) {
                            if (!processor.isFinished()) {
                                element = requireNonNull(processor.getResult(), "result is null");
                            }
                        }
                        else if (processor.isBlocked()) {
                            return ProcessState.blocked(processor.getBlockedFuture());
                        }
                        else {
                            return ProcessState.yielded();
                        }
                    }

                    TransformationState<R> state = requireNonNull(transformation.process(element), "state is null");

                    if (state.isNeedsMoreData()) {
                        checkState(!processor.isFinished(), "Cannot request more data when base processor is finished");
                        // set element to empty() in order to fetch a new one
                        element = null;
                    }

                    // pass-through transformation state if it doesn't require new data
                    switch (state.getType()) {
                        case NEEDS_MORE_DATA:
                            break;
                        case BLOCKED:
                            return ProcessState.blocked(state.getBlocked());
                        case YIELD:
                            return ProcessState.yielded();
                        case RESULT:
                            return ProcessState.ofResult(state.getResult());
                        case FINISHED:
                            return ProcessState.finished();
                    }
                }
            }
        });
    }

    static <T> WorkProcessor<T> create(WorkProcessor.Process<T> process)
    {
        return new ProcessWorkProcessor<>(process);
    }

    private static class ProcessWorkProcessor<T>
            implements WorkProcessor<T>
    {
        @Nullable
        WorkProcessor.Process<T> process;
        // set initial state to yield as it will cause processor computations to progress
        ProcessState<T> state = ProcessState.yielded();

        ProcessWorkProcessor(WorkProcessor.Process<T> process)
        {
            this.process = requireNonNull(process, "process is null");
        }

        @Override
        public boolean process()
        {
            if (isBlocked()) {
                return false;
            }
            if (isFinished()) {
                return true;
            }
            state = requireNonNull(process.process());

            if (state.getType() == ProcessState.Type.FINISHED) {
                process = null;
                return true;
            }

            return state.getType() == ProcessState.Type.RESULT;
        }

        @Override
        public boolean isBlocked()
        {
            return state.getType() == ProcessState.Type.BLOCKED && !state.getBlocked().isDone();
        }

        @Override
        public ListenableFuture<Void> getBlockedFuture()
        {
            checkState(state.getType() == ProcessState.Type.BLOCKED, "Must be blocked to get blocked future");
            return state.getBlocked();
        }

        @Override
        public boolean isFinished()
        {
            return state.getType() == ProcessState.Type.FINISHED;
        }

        @Override
        public T getResult()
        {
            checkState(state.getType() == ProcessState.Type.RESULT, "process() must return true and must not be finished");
            return state.getResult();
        }
    }

    private static class ElementAndProcessor<T>
    {
        @Nullable final T element;
        final WorkProcessor<T> processor;

        ElementAndProcessor(T element, WorkProcessor<T> processor)
        {
            this.element = element;
            this.processor = requireNonNull(processor, "processor is null");
        }

        T getElement()
        {
            return element;
        }

        WorkProcessor<T> getProcessor()
        {
            return processor;
        }
    }
}
