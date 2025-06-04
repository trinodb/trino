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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.operator.WorkProcessorAssertion.Transform;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.operator.WorkProcessor.ProcessState.Type.BLOCKED;
import static io.trino.operator.WorkProcessor.ProcessState.Type.FINISHED;
import static io.trino.operator.WorkProcessor.ProcessState.Type.RESULT;
import static io.trino.operator.WorkProcessor.ProcessState.Type.YIELD;
import static io.trino.operator.WorkProcessorAssertion.assertBlocks;
import static io.trino.operator.WorkProcessorAssertion.assertFinishes;
import static io.trino.operator.WorkProcessorAssertion.assertResult;
import static io.trino.operator.WorkProcessorAssertion.assertUnblocks;
import static io.trino.operator.WorkProcessorAssertion.assertYields;
import static io.trino.operator.WorkProcessorAssertion.processorFrom;
import static io.trino.operator.WorkProcessorAssertion.transformationFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestWorkProcessor
{
    @Test
    public void testIterator()
    {
        WorkProcessor<Integer> processor = processorFrom(ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.finished()));

        Iterator<Integer> iterator = processor.iterator();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo((Integer) 1);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo((Integer) 2);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testIteratorFailsWhenWorkProcessorHasYielded()
    {
        // iterator should fail if underlying work has yielded
        WorkProcessor<Integer> processor = processorFrom(ImmutableList.of(ProcessState.yielded()));
        Iterator<Integer> iterator = processor.iterator();
        //noinspection ResultOfMethodCallIgnored
        assertThatThrownBy(iterator::hasNext)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot iterate over yielding WorkProcessor");
    }

    @Test
    public void testIteratorFailsWhenWorkProcessorIsBlocked()
    {
        // iterator should fail if underlying work is blocked
        WorkProcessor<Integer> processor = processorFrom(ImmutableList.of(ProcessState.blocked(SettableFuture.create())));
        Iterator<Integer> iterator = processor.iterator();
        //noinspection ResultOfMethodCallIgnored
        assertThatThrownBy(iterator::hasNext)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot iterate over blocking WorkProcessor");
    }

    @Test
    @Timeout(10)
    public void testMergeSorted()
    {
        List<ProcessState<Integer>> firstStream = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(3),
                ProcessState.yielded(),
                ProcessState.ofResult(5),
                ProcessState.finished());

        SettableFuture<Void> secondFuture = SettableFuture.create();
        List<ProcessState<Integer>> secondStream = ImmutableList.of(
                ProcessState.ofResult(2),
                ProcessState.ofResult(4),
                ProcessState.blocked(secondFuture),
                ProcessState.finished());

        WorkProcessor<Integer> mergedStream = WorkProcessorUtils.mergeSorted(
                ImmutableList.of(processorFrom(firstStream), processorFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        // first stream result 1
        assertResult(mergedStream, 1);

        // second stream result 2
        assertResult(mergedStream, 2);

        // first stream result 3
        assertResult(mergedStream, 3);

        // first stream yield
        assertYields(mergedStream);

        // second stream result 4
        assertResult(mergedStream, 4);

        // second stream blocked
        assertBlocks(mergedStream);

        // second stream unblock
        assertUnblocks(mergedStream, secondFuture);

        // first stream result 5
        assertResult(mergedStream, 5);

        // both streams finished
        assertFinishes(mergedStream);
    }

    @Test
    @Timeout(10)
    public void testMergeSortedEmptyStreams()
    {
        SettableFuture<Void> firstFuture = SettableFuture.create();
        List<ProcessState<Integer>> firstStream = ImmutableList.of(
                ProcessState.blocked(firstFuture),
                ProcessState.yielded(),
                ProcessState.finished());

        SettableFuture<Void> secondFuture = SettableFuture.create();
        List<ProcessState<Integer>> secondStream = ImmutableList.of(
                ProcessState.blocked(secondFuture),
                ProcessState.finished());

        WorkProcessor<Integer> mergedStream = WorkProcessorUtils.mergeSorted(
                ImmutableList.of(processorFrom(firstStream), processorFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        assertThat(mergedStream.isBlocked()).isFalse();
        assertThat(mergedStream.isFinished()).isFalse();

        // first stream blocked
        assertBlocks(mergedStream);

        // first stream unblock
        assertUnblocks(mergedStream, firstFuture);

        // first stream yield
        assertYields(mergedStream);

        // second stream blocked
        assertBlocks(mergedStream);

        // first stream unblock
        assertUnblocks(mergedStream, secondFuture);

        // both streams finished
        assertFinishes(mergedStream);
    }

    @Test
    @Timeout(10)
    public void testMergeSortedEmptyStreamsWithFinishedOnly()
    {
        List<ProcessState<Integer>> firstStream = ImmutableList.of(
                ProcessState.finished());

        List<ProcessState<Integer>> secondStream = ImmutableList.of(
                ProcessState.finished());

        WorkProcessor<Integer> mergedStream = WorkProcessorUtils.mergeSorted(
                ImmutableList.of(processorFrom(firstStream), processorFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        // before
        assertThat(mergedStream.isBlocked()).isFalse();
        assertThat(mergedStream.isFinished()).isFalse();

        assertFinishes(mergedStream);
    }

    @Test
    @Timeout(10)
    public void testYield()
    {
        SettableFuture<Void> future = SettableFuture.create();

        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.blocked(future),
                ProcessState.ofResult(3),
                ProcessState.ofResult(4),
                ProcessState.finished());

        AtomicBoolean yieldSignal = new AtomicBoolean();
        WorkProcessor<Integer> processor = processorFrom(baseScenario)
                .yielding(yieldSignal::get);

        // no yield, process normally
        assertResult(processor, 1);

        yieldSignal.set(true);
        assertYields(processor);

        // processor should progress since it yielded last time
        assertResult(processor, 2);

        // yield signal is still set
        assertYields(processor);

        // base scenario future blocks
        assertBlocks(processor);
        assertUnblocks(processor, future);

        // continue to process normally
        yieldSignal.set(false);
        assertResult(processor, 3);
        assertResult(processor, 4);
        assertFinishes(processor);
    }

    @Test
    @Timeout(10)
    public void testBlock()
    {
        SettableFuture<Void> phase1 = SettableFuture.create();

        List<ProcessState<Integer>> scenario = ImmutableList.of(
                ProcessState.blocked(phase1),
                ProcessState.yielded(),
                ProcessState.ofResult(1),
                ProcessState.finished());

        AtomicReference<SettableFuture<Void>> phase2 = new AtomicReference<>(SettableFuture.create());
        WorkProcessor<Integer> processor = processorFrom(scenario)
                .blocking(phase2::get);

        // Make sure processor is initially blocked
        assertThat(processor.isBlocked()).isTrue();

        // WorkProcessor.blocking future overrides phase1 future
        assertBlocks(processor);
        assertUnblocks(processor, phase2.get());

        assertBlocks(processor);
        assertUnblocks(processor, phase1);

        // WorkProcessor.blocking overrides yielding
        phase2.set(SettableFuture.create());
        assertBlocks(processor);
        assertUnblocks(processor, phase2.get());
        assertResult(processor, 1);

        // WorkProcessor.blocking overrides finishing
        phase2.set(SettableFuture.create());
        assertBlocks(processor);
        assertUnblocks(processor, phase2.get());
        assertFinishes(processor);
    }

    @Test
    @Timeout(10)
    public void testProcessStateMonitor()
    {
        SettableFuture<Void> future = SettableFuture.create();

        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.yielded(),
                ProcessState.blocked(future),
                ProcessState.finished());

        ImmutableList.Builder<ProcessState.Type> actions = ImmutableList.builder();

        WorkProcessor<Integer> processor = processorFrom(baseScenario)
                .withProcessStateMonitor(state -> actions.add(state.getType()));

        assertResult(processor, 1);
        assertYields(processor);
        assertBlocks(processor);
        assertUnblocks(processor, future);
        assertFinishes(processor);

        assertThat(actions.build()).isEqualTo(ImmutableList.of(RESULT, YIELD, BLOCKED, FINISHED));
    }

    @Test
    @Timeout(10)
    public void testFinished()
    {
        AtomicBoolean finished = new AtomicBoolean();
        SettableFuture<Void> future = SettableFuture.create();

        List<ProcessState<Integer>> scenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.yielded(),
                ProcessState.blocked(future),
                ProcessState.ofResult(2));

        WorkProcessor<Integer> processor = processorFrom(scenario)
                .finishWhen(finished::get);

        assertResult(processor, 1);
        assertYields(processor);
        assertBlocks(processor);

        finished.set(true);
        assertBlocks(processor);

        assertUnblocks(processor, future);
        assertFinishes(processor);
    }

    @Test
    @Timeout(10)
    public void testFlatMap()
    {
        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.finished());

        WorkProcessor<Double> processor = processorFrom(baseScenario)
                .flatMap(element -> WorkProcessor.fromIterable(ImmutableList.of((Double) 2. * element, (Double) 3. * element)));

        assertResult(processor, 2.);
        assertResult(processor, 3.);
        assertResult(processor, 4.);
        assertResult(processor, 6.);
        assertFinishes(processor);
    }

    @Test
    @Timeout(10)
    public void testMap()
    {
        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.finished());

        WorkProcessor<Double> processor = processorFrom(baseScenario)
                .map(element -> 2. * element);

        assertResult(processor, 2.);
        assertResult(processor, 4.);
        assertFinishes(processor);
    }

    @Test
    @Timeout(10)
    public void testFlatTransform()
    {
        SettableFuture<Void> baseFuture = SettableFuture.create();
        List<ProcessState<Double>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1.0),
                ProcessState.blocked(baseFuture),
                ProcessState.ofResult(2.0),
                ProcessState.yielded(),
                ProcessState.ofResult(3.0),
                ProcessState.ofResult(4.0),
                ProcessState.finished());

        SettableFuture<Void> mappedFuture1 = SettableFuture.create();
        List<ProcessState<Integer>> mappedScenario1 = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.yielded(),
                ProcessState.blocked(mappedFuture1),
                ProcessState.ofResult(2),
                ProcessState.finished());

        List<ProcessState<Integer>> mappedScenario2 = ImmutableList.of(ProcessState.finished());

        SettableFuture<Void> mappedFuture3 = SettableFuture.create();
        List<ProcessState<Integer>> mappedScenario3 = ImmutableList.of(
                ProcessState.blocked(mappedFuture3),
                ProcessState.finished());

        List<ProcessState<Integer>> mappedScenario4 = ImmutableList.of(
                ProcessState.ofResult(3),
                ProcessState.finished());

        SettableFuture<Void> transformationFuture = SettableFuture.create();
        List<Transform<Double, WorkProcessor<Integer>>> transformationScenario = ImmutableList.of(
                Transform.of(Optional.of(1.0), TransformationState.ofResult(processorFrom(mappedScenario1), false)),
                Transform.of(Optional.of(1.0), TransformationState.ofResult(processorFrom(mappedScenario2), false)),
                Transform.of(Optional.of(1.0), TransformationState.ofResult(processorFrom(mappedScenario3))),
                Transform.of(Optional.of(2.0), TransformationState.blocked(transformationFuture)),
                Transform.of(Optional.of(2.0), TransformationState.ofResult(processorFrom(mappedScenario4))),
                Transform.of(Optional.of(3.0), TransformationState.finished()));

        WorkProcessor<Integer> processor = processorFrom(baseScenario)
                .flatTransform(transformationFrom(transformationScenario));

        // mappedScenario1.result 1
        assertResult(processor, 1);

        // mappedScenario1.yield
        assertYields(processor);

        // mappedScenario1.blocked
        assertBlocks(processor);

        // mappedScenario1 unblocks
        assertUnblocks(processor, mappedFuture1);

        // mappedScenario1 result 2
        assertResult(processor, 2);

        // mappedScenario3.blocked
        assertBlocks(processor);

        // mappedScenario3 unblocks
        assertUnblocks(processor, mappedFuture3);

        // base.blocked
        assertBlocks(processor);

        // base unblocks
        assertUnblocks(processor, baseFuture);

        // transformation.blocked
        assertBlocks(processor);

        // transformation unblocks
        assertUnblocks(processor, transformationFuture);

        // mappedScenario4 result 3
        assertResult(processor, 3);

        // base.yield
        assertYields(processor);

        // transformation finishes
        assertFinishes(processor);
    }

    @Test
    @Timeout(10)
    public void testTransform()
    {
        SettableFuture<Void> baseFuture = SettableFuture.create();
        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.yielded(),
                ProcessState.blocked(baseFuture),
                ProcessState.ofResult(2),
                ProcessState.ofResult(3),
                ProcessState.finished());

        SettableFuture<Void> transformationFuture = SettableFuture.create();
        List<Transform<Integer, String>> transformationScenario = ImmutableList.of(
                Transform.of(Optional.of(1), TransformationState.needsMoreData()),
                Transform.of(Optional.of(2), TransformationState.ofResult("foo")),
                Transform.of(Optional.of(3), TransformationState.blocked(transformationFuture)),
                Transform.of(Optional.of(3), TransformationState.yielded()),
                Transform.of(Optional.of(3), TransformationState.ofResult("bar", false)),
                Transform.of(Optional.of(3), TransformationState.ofResult("zoo", true)),
                Transform.of(Optional.empty(), TransformationState.ofResult("car", false)),
                Transform.of(Optional.empty(), TransformationState.finished()));

        WorkProcessor<String> processor = processorFrom(baseScenario)
                .transform(transformationFrom(transformationScenario));

        // before
        assertThat(processor.isBlocked()).isFalse();
        assertThat(processor.isFinished()).isFalse();

        // base.yield
        assertYields(processor);

        // base.blocked
        assertBlocks(processor);

        // base unblock
        assertUnblocks(processor, baseFuture);

        // transformation.result foo
        assertResult(processor, "foo");

        // transformation.blocked
        assertBlocks(processor);

        // transformation.unblock
        assertUnblocks(processor, transformationFuture);

        // transformation.yield
        assertYields(processor);

        // transformation.result bar
        assertResult(processor, "bar");

        // transformation.result zoo
        assertResult(processor, "zoo");

        // transformation.result car
        assertResult(processor, "car");

        // transformation.finished
        assertFinishes(processor);
    }

    @Test
    @Timeout(10)
    public void testCreateFrom()
    {
        SettableFuture<Void> future = SettableFuture.create();
        List<ProcessState<Integer>> scenario = ImmutableList.of(
                ProcessState.yielded(),
                ProcessState.ofResult(1),
                ProcessState.blocked(future),
                ProcessState.yielded(),
                ProcessState.ofResult(2),
                ProcessState.finished());
        WorkProcessor<Integer> processor = processorFrom(scenario);

        // before
        assertThat(processor.isBlocked()).isFalse();
        assertThat(processor.isFinished()).isFalse();

        assertYields(processor);
        assertResult(processor, 1);
        assertBlocks(processor);
        assertUnblocks(processor, future);
        assertYields(processor);
        assertResult(processor, 2);
        assertFinishes(processor);
    }
}
