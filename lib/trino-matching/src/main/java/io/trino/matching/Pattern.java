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
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

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
    private final Pattern<?> previousBranch;

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
        this.previousBranch = previous.isEmpty() ? null : ChainedMatchCursor.findBranch(previous.orElseThrow());
    }

    final Pattern<?> previousBranch()
    {
        return previousBranch;
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

    public <C> boolean matches(Object object, C context)
    {
        MatchCursor cursor = matchCursor(object, context);
        return cursor.matches();
    }

    public final MatchCursor matchCursor(Object object)
    {
        return matchCursor(object, Captures.empty(), null);
    }

    public final <C> MatchCursor matchCursor(Object object, C context)
    {
        return matchCursor(object, Captures.empty(), context);
    }

    public final <C> MatchCursor matchCursor(Object object, Captures captures, C context)
    {
        requireNonNull(captures, "captures is null");
        Pattern<?> branch = ChainedMatchCursor.findBranch(this);
        if (branch == null) {
            return new SingleMatchCursor(this, null, object, captures, context);
        }
        return new ChainedMatchCursor(this, branch, object, captures, context);
    }

    @Override
    public String toString()
    {
        PatternPrinter printer = new PatternPrinter();
        printer.print(this);
        return printer.result();
    }
}
