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
package io.prestosql.operator;

import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.TransformationState;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class WorkProcessorAssertion
{
    private WorkProcessorAssertion() {}

    public static <T> void assertBlocks(WorkProcessor<T> processor)
    {
        assertFalse(processor.process());
        assertTrue(processor.isBlocked());
        assertFalse(processor.isFinished());
        assertFalse(processor.process());
    }

    public static <T, V> void assertUnblocks(WorkProcessor<T> processor, SettableFuture<V> future)
    {
        future.set(null);
        assertFalse(processor.isBlocked());
    }

    public static <T> void assertYields(WorkProcessor<T> processor)
    {
        assertFalse(processor.process());
        assertFalse(processor.isBlocked());
        assertFalse(processor.isFinished());
    }

    public static <T> void assertResult(WorkProcessor<T> processor, T result)
    {
        validateResult(processor, actualResult -> assertEquals(processor.getResult(), result));
    }

    public static <T> void validateResult(WorkProcessor<T> processor, Consumer<T> validator)
    {
        assertTrue(processor.process());
        assertFalse(processor.isBlocked());
        assertFalse(processor.isFinished());
        validator.accept(processor.getResult());
    }

    public static <T> void assertFinishes(WorkProcessor<T> processor)
    {
        assertTrue(processor.process());
        assertFalse(processor.isBlocked());
        assertTrue(processor.isFinished());
        assertTrue(processor.process());
    }

    public static <T, R> WorkProcessor.Transformation<T, R> transformationFrom(List<Transform<T, R>> transformations)
    {
        return transformationFrom(transformations, Objects::equals);
    }

    public static <T, R> WorkProcessor.Transformation<T, R> transformationFrom(List<Transform<T, R>> transformations, BiPredicate<T, T> equalsPredicate)
    {
        Iterator<Transform<T, R>> iterator = transformations.iterator();
        return element -> {
            assertTrue(iterator.hasNext());
            return iterator.next().transform(
                    Optional.ofNullable(element),
                    (left, right) -> left.isPresent() == right.isPresent()
                            && (!left.isPresent() || equalsPredicate.test(left.get(), right.get())));
        };
    }

    public static <T> WorkProcessor<T> processorFrom(List<ProcessState<T>> states)
    {
        Iterator<ProcessState<T>> iterator = states.iterator();
        return WorkProcessorUtils.create(() -> {
            assertTrue(iterator.hasNext());
            return iterator.next();
        });
    }

    public static class Transform<T, R>
    {
        private final Optional<T> from;
        private final TransformationState<R> to;

        public static <T, R> Transform<T, R> of(Optional<T> from, TransformationState<R> to)
        {
            return new Transform<>(from, to);
        }

        private Transform(Optional<T> from, TransformationState<R> to)
        {
            this.from = requireNonNull(from);
            this.to = requireNonNull(to);
        }

        private TransformationState<R> transform(Optional<T> from, BiPredicate<Optional<T>, Optional<T>> equalsPredicate)
        {
            assertTrue(equalsPredicate.test(from, this.from), format("Expected %s to be equal to %s", from, this.from));
            return to;
        }
    }
}
