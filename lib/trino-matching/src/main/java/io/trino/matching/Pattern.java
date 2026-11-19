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
package io.trino.matching;

import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;

public abstract sealed class Pattern<T>
        permits CapturePattern,
                CustomPattern,
                EqualsPattern,
                FilterPattern,
                OrPattern,
                TypeOfPattern,
                WithPattern
{
    private final Optional<Pattern<?>> previous;
    private final boolean singleMatch;

    public static Pattern<Object> any()
    {
        return typeOf(Object.class);
    }

    public static <T> Pattern<T> typeOf(Class<T> expectedClass)
    {
        return new TypeOfPattern<>(expectedClass);
    }

    protected Pattern(Pattern<?> previous)
    {
        this(Optional.of(requireNonNull(previous, "previous is null")));
    }

    protected Pattern(Optional<Pattern<?>> previous)
    {
        this(previous, false);
    }

    Pattern(Optional<Pattern<?>> previous, boolean singleMatch)
    {
        this.previous = requireNonNull(previous, "previous is null");
        this.singleMatch = singleMatch && (previous.isEmpty() || previous.orElseThrow().singleMatch);
    }

    final boolean isSingleMatch()
    {
        return singleMatch;
    }

    // FIXME make sure there's a proper toString,
    // like with(propName)\n\tfilter(isEmpty)
    // or with(propName) map(isEmpty) equalTo(true)
    public static <F, C, T extends Iterable<S>, S> PropertyPattern<F, C, T> empty(Property<F, C, T> property)
    {
        return PropertyPattern.upcast(property.matching(Iterables::isEmpty));
    }

    public static <F, C, T extends Iterable<S>, S> PropertyPattern<F, C, T> nonEmpty(Property<F, C, T> property)
    {
        return PropertyPattern.upcast(property.matching(not(Iterables::isEmpty)));
    }

    public Pattern<T> capturedAs(Capture<T> capture)
    {
        return new CapturePattern<>(capture, this);
    }

    public Pattern<T> matching(Predicate<? super T> predicate)
    {
        return matching((t, _) -> predicate.test(t));
    }

    public Pattern<T> matching(BiPredicate<? super T, ?> predicate)
    {
        return new FilterPattern<>(predicate, Optional.of(this));
    }

    public Pattern<T> with(PropertyPattern<? super T, ?, ?> pattern)
    {
        return new WithPattern<>(pattern, this);
    }

    @SafeVarargs
    public final Pattern<T> or(Function<Pattern<T>, Pattern<T>>... patternsSupplier)
    {
        List<Pattern<T>> patterns = Arrays.stream(patternsSupplier)
                .map(supplier -> supplier.apply(this))
                .collect(toImmutableList());
        return new OrPattern<>(patterns, this);
    }

    public Optional<Pattern<?>> previous()
    {
        return previous;
    }

    public abstract <C> Stream<Match> accept(Object object, Captures captures, C context);

    public <C> boolean matches(Object object, C context)
    {
        return match(object, context)
                .findFirst()
                .isPresent();
    }

    public final Stream<Match> match(Object object)
    {
        return match(object, Captures.empty(), null);
    }

    public final <C> Stream<Match> match(Object object, C context)
    {
        return match(object, Captures.empty(), context);
    }

    public final <C> Stream<Match> match(Object object, Captures captures, C context)
    {
        if (singleMatch) {
            return StreamSupport.stream(new Spliterators.AbstractSpliterator<Match>(1, ORDERED | NONNULL)
            {
                private boolean consumed;

                @Override
                public boolean tryAdvance(Consumer<? super Match> action)
                {
                    requireNonNull(action, "action is null");
                    if (consumed) {
                        return false;
                    }
                    consumed = true;
                    Captures result = matchCaptures(object, captures, context);
                    if (result == null) {
                        return false;
                    }
                    action.accept(Match.of(result));
                    return true;
                }
            }, false);
        }
        if (previous.isPresent()) {
            return previous.get().match(object, captures, context)
                    .flatMap(match -> accept(object, match.captures(), context));
        }
        return accept(object, captures, context);
    }

    @SuppressWarnings("unchecked")
    private <C> Captures matchCaptures(Object object, Captures captures, C context)
    {
        if (previous.isPresent()) {
            captures = previous.orElseThrow().matchCaptures(object, captures, context);
            if (captures == null) {
                return null;
            }
        }
        return switch (this) {
            case TypeOfPattern<?> pattern -> pattern.expectedClass().isInstance(object) ? captures : null;
            case EqualsPattern<?> pattern -> pattern.expectedValue().equals(object) ? captures : null;
            case FilterPattern<?> pattern -> ((BiPredicate<Object, C>) pattern.predicate()).test(object, context) ? captures : null;
            case CapturePattern<?> pattern -> captures.addAll(Captures.ofNullable((Capture<Object>) pattern.capture(), object));
            case WithPattern<?> pattern -> {
                BiFunction<Object, C, Optional<?>> property = (BiFunction<Object, C, Optional<?>>) pattern.getProperty().getFunction();
                Optional<?> value = property.apply(object, context);
                yield value.isPresent() ? pattern.getPattern().matchCaptures(value.orElseThrow(), captures, context) : null;
            }
            case OrPattern<?> _, CustomPattern<?> _ -> throw new IllegalStateException("Pattern can produce multiple matches");
        };
    }

    @Override
    public String toString()
    {
        PatternPrinter printer = new PatternPrinter();
        printer.print(this);
        return printer.result();
    }
}
