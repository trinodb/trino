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
import io.trino.matching.pattern.CapturePattern;
import io.trino.matching.pattern.FilterPattern;
import io.trino.matching.pattern.OrPattern;
import io.trino.matching.pattern.TypeOfPattern;
import io.trino.matching.pattern.WithPattern;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public abstract class Pattern<T>
{
    private final Optional<Pattern<?>> previous;

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
        this.previous = requireNonNull(previous, "previous is null");
    }

    //FIXME make sure there's a proper toString,
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
        return matching((t, context) -> predicate.test(t));
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

    public abstract void accept(PatternVisitor patternVisitor);

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
        if (previous.isPresent()) {
            return previous.get().match(object, captures, context)
                    .flatMap(match -> accept(object, match.captures(), context));
        }
        return accept(object, captures, context);
    }

    @Override
    public String toString()
    {
        DefaultPrinter printer = new DefaultPrinter();
        accept(printer);
        return printer.result();
    }
}
