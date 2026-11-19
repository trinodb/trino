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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.matching.Capture.newCapture;
import static io.trino.matching.Pattern.typeOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMatchCursor
{
    @Test
    public void testSingleMatchIsLazy()
    {
        AtomicInteger calls = new AtomicInteger();
        Capture<Integer> capture = newCapture();
        Pattern<Integer> pattern = typeOf(Integer.class)
                .matching(_ -> calls.incrementAndGet() == 1)
                .capturedAs(capture);
        MatchCursor cursor = pattern.matchCursor(7);
        assertThat(cursor).isInstanceOf(SingleMatchCursor.class);
        assertThat(calls.get()).isZero();
        assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        assertThat(cursor.matches()).isTrue();
        Captures retained = cursor.current();
        assertThat(retained.get(capture)).isEqualTo(7);
        assertThat(cursor.matches()).isFalse();
        assertThat(cursor.matches()).isFalse();
        assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        assertThat(retained.get(capture)).isEqualTo(7);

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    public void testShortCircuitAndLazyConstruction()
    {
        AtomicInteger calls = new AtomicInteger();
        Pattern<Integer> pattern = typeOf(Integer.class)
                .matching(value -> value > 0)
                .with(Property.property("value", (Integer value) -> {
                    calls.incrementAndGet();
                    return value;
                }).equalTo(1));
        for (Object value : List.of(-1, "wrong type")) {
            MatchCursor cursor = pattern.matchCursor(value);
            assertThat(cursor.matches()).isFalse();
            assertThat(cursor.matches()).isFalse();
        }
        MatchCursor cursor = pattern.matchCursor(1);
        assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        assertThat(calls.get()).isZero();
        assertThat(typeOf(Integer.class).matches(null, null)).isFalse();
    }

    @Test
    public void testAlternativesAreEvaluatedOnDemand()
    {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        Pattern<Integer> pattern = typeOf(Integer.class).or(
                p -> p.matching(_ -> first.incrementAndGet() > 0),
                p -> p.matching(_ -> second.incrementAndGet() > 0));
        MatchCursor cursor = pattern.matchCursor(1);
        assertThat(first.get()).isZero();
        assertThat(second.get()).isZero();
        assertThat(cursor.matches()).isTrue();
        assertThat(first.get()).isEqualTo(1);
        assertThat(second.get()).isZero();
        assertThat(cursor.matches()).isTrue();
        assertThat(second.get()).isEqualTo(1);
        assertThat(cursor.matches()).isFalse();
    }

    @Test
    public void testLaterAlternativeCanThrowWithoutAffectingFirstMatch()
    {
        Pattern<Integer> pattern = typeOf(Integer.class).or(
                p -> p.matching(_ -> true),
                p -> p.matching(_ -> { throw new IllegalArgumentException("later alternative"); }));
        assertThat(pattern.matches(1, null)).isTrue();
        MatchCursor cursor = pattern.matchCursor(1);
        assertThat(cursor.matches()).isTrue();
        assertThatThrownBy(cursor::matches)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("later alternative");
    }

    @Test
    public void testSingleMatchPrefixIsEvaluatedOnce()
    {
        AtomicInteger prefix = new AtomicInteger();
        Pattern<Integer> pattern = typeOf(Integer.class)
                .matching(value -> prefix.incrementAndGet() > 0 && value > 0)
                .or(p -> p.matching(_ -> true), p -> p.matching(_ -> true));
        MatchCursor cursor = pattern.matchCursor(1);
        assertThat(cursor).isInstanceOf(ChainedMatchCursor.class);
        assertThat(prefix.get()).isZero();
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.matches()).isFalse();
        assertThat(cursor.matches()).isFalse();
        assertThat(prefix.get()).isEqualTo(1);

        MatchCursor rejected = pattern.matchCursor(-1);
        assertThat(rejected.matches()).isFalse();
        assertThat(rejected.matches()).isFalse();
        assertThat(prefix.get()).isEqualTo(2);
    }

    @Test
    public void testMultiMatchPrefixIsExhaustedOnce()
    {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        Pattern<Integer> pattern = typeOf(Integer.class)
                .or(p -> p.matching(_ -> first.incrementAndGet() > 0), p -> p.matching(_ -> first.incrementAndGet() < 0))
                .or(p -> p.matching(_ -> second.incrementAndGet() > 0), p -> p.matching(_ -> second.incrementAndGet() > 0));
        MatchCursor cursor = pattern.matchCursor(1);
        assertThat(first.get()).isZero();
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.matches()).isFalse();
        assertThat(cursor.matches()).isFalse();
        assertThat(first.get()).isEqualTo(2);
        assertThat(second.get()).isEqualTo(2);
    }

    @Test
    public void testNestedMatchesFormAnOrderedProduct()
    {
        Capture<Integer> first = newCapture();
        Capture<Integer> second = newCapture();
        AtomicInteger properties = new AtomicInteger();
        Property<String, ?, Integer> length = Property.property("length", (String value) -> {
            properties.incrementAndGet();
            return value.length();
        });
        Pattern<String> pattern = typeOf(String.class)
                .with(length.matching(values(first, new AtomicInteger(), new AtomicInteger())))
                .with(length.matching(values(second, new AtomicInteger(), new AtomicInteger())));
        List<Captures> results = new ArrayList<>();
        MatchCursor cursor = pattern.matchCursor("abc");
        assertThat(properties.get()).isZero();
        while (cursor.matches()) {
            results.add(cursor.current());
        }

        assertThat(results.stream().map(c -> List.of(c.get(first), c.get(second))).toList())
                .containsExactly(List.of(1, 1), List.of(1, 2), List.of(2, 1), List.of(2, 2));
        assertThat(properties.get()).isEqualTo(3);
    }

    @Test
    public void testChainedAlternativesWithSingleMatchContinuations()
    {
        Capture<Integer> first = newCapture();
        Capture<Integer> second = newCapture();
        Capture<Integer> root = newCapture();
        AtomicInteger properties = new AtomicInteger();
        Pattern<Integer> pattern = typeOf(Integer.class)
                .or(
                        p -> p.with(Property.property("first", (Integer _) -> 1).capturedAs(first)),
                        p -> p.with(Property.property("first", (Integer _) -> 2).capturedAs(first)))
                .with(Property.property("identity", (Integer value) -> {
                    properties.incrementAndGet();
                    return value;
                }).equalTo(9))
                .or(
                        p -> p.with(Property.property("second", (Integer _) -> 10).capturedAs(second)),
                        p -> p.with(Property.property("second", (Integer _) -> 20).capturedAs(second)))
                .capturedAs(root);
        MatchCursor cursor = pattern.matchCursor(9);
        assertThat(properties.get()).isZero();
        List<List<Integer>> results = new ArrayList<>();
        while (cursor.matches()) {
            Captures captures = cursor.current();
            results.add(List.of(captures.get(first), captures.get(second), captures.get(root)));
        }
        assertThat(results).containsExactly(List.of(1, 10, 9), List.of(1, 20, 9), List.of(2, 10, 9), List.of(2, 20, 9));
        assertThat(properties.get()).isEqualTo(2);
    }

    @Test
    public void testNestedAlternativesPreserveDuplicateMatches()
    {
        Capture<Integer> capture = newCapture();
        AtomicInteger calls = new AtomicInteger();
        Pattern<Integer> alternatives = typeOf(Integer.class)
                .or(p -> p.matching(value -> value > 0), p -> p.matching(value -> value % 2 == 0))
                .capturedAs(capture);
        Pattern<String> pattern = typeOf(String.class)
                .with(Property.property("length", (String value) -> {
                    calls.incrementAndGet();
                    return value.length();
                }).matching(alternatives));
        MatchCursor cursor = pattern.matchCursor("ab");
        assertThat(calls.get()).isZero();
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.current().get(capture)).isEqualTo(2);
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.current().get(capture)).isEqualTo(2);
        assertThat(cursor.matches()).isFalse();

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    public void testRejectedContinuationResumesPreviousMatches()
    {
        Capture<Integer> capture = newCapture();
        AtomicInteger calls = new AtomicInteger();
        Pattern<Integer> pattern = values(capture, new AtomicInteger(), new AtomicInteger())
                .matching(_ -> calls.incrementAndGet() == 2);
        MatchCursor cursor = pattern.matchCursor(9);
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.current().get(capture)).isEqualTo(2);
        assertThat(calls.get()).isEqualTo(2);
        assertThat(cursor.matches()).isFalse();
    }

    @Test
    public void testCustomPatternIsLazyAndStopsOnEarlyExit()
    {
        Capture<Integer> capture = newCapture();
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        Pattern<Integer> pattern = values(capture, opens, reads);
        MatchCursor cursor = pattern.matchCursor(9);
        assertThat(opens.get()).isZero();
        assertThat(cursor.matches()).isTrue();
        assertThat(opens.get()).isEqualTo(1);
        assertThat(reads.get()).isEqualTo(1);
        assertThat(cursor.current().get(capture)).isEqualTo(1);

        assertThat(reads.get()).isEqualTo(1);
    }

    @Test
    public void testMatchesStopsAfterFirstCustomMatch()
    {
        AtomicInteger reads = new AtomicInteger();
        assertThat(values(newCapture(), new AtomicInteger(), reads).matches(9, null)).isTrue();
        assertThat(reads.get()).isEqualTo(1);
    }

    @Test
    public void testCustomMatchesRemainLazyAndOrdered()
    {
        Capture<Integer> capture = newCapture();
        AtomicInteger reads = new AtomicInteger();
        Pattern<Integer> pattern = values(capture, new AtomicInteger(), reads);
        MatchCursor cursor = pattern.matchCursor(9);
        assertThat(reads.get()).isZero();
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.current().get(capture)).isEqualTo(1);
        assertThat(reads.get()).isEqualTo(1);
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.current().get(capture)).isEqualTo(2);
        assertThat(cursor.matches()).isFalse();
        assertThat(cursor.matches()).isFalse();
        assertThat(reads.get()).isEqualTo(2);
    }

    @Test
    public void testEmptyNestedPropertyAndAlternatives()
    {
        Pattern<Integer> alternatives = typeOf(Integer.class).or();
        MatchCursor cursor = alternatives.matchCursor(1);
        assertThat(cursor.matches()).isFalse();
        assertThat(cursor.matches()).isFalse();

        Pattern<String> pattern = typeOf(String.class)
                .with(Property.optionalProperty("absent", (String _) -> Optional.<Integer>empty()).matching(alternatives));
        assertThat(pattern.matches("value", null)).isFalse();
    }

    @Test
    public void testNestedCursorDoesNotEvaluateLaterMatches()
    {
        AtomicInteger reads = new AtomicInteger();
        Pattern<Integer> pattern = values(newCapture(), new AtomicInteger(), reads)
                .with(Property.property("identity", (Integer value) -> value)
                        .matching(values(newCapture(), new AtomicInteger(), reads)));
        MatchCursor cursor = pattern.matchCursor(9);
        assertThat(cursor.matches()).isTrue();
        assertThat(reads.get()).isEqualTo(2);
        assertThat(cursor.matches()).isTrue();
        assertThat(reads.get()).isEqualTo(3);
    }

    @Test
    public void testContextAndInitialCaptures()
    {
        Object context = new Object();
        Capture<Integer> initial = newCapture();
        Capture<Integer> capture = newCapture();
        Pattern<Integer> pattern = typeOf(Integer.class)
                .matching((Integer _, Object actualContext) -> actualContext == context)
                .with(Property.property("context", (Integer _, Object actualContext) -> actualContext).equalTo(context))
                .capturedAs(capture);
        MatchCursor cursor = pattern.matchCursor(2, Captures.ofNullable(initial, 1), context);
        assertThat(cursor.matches()).isTrue();
        assertThat(cursor.current().get(initial)).isEqualTo(1);
        assertThat(cursor.current().get(capture)).isEqualTo(2);
    }

    @Test
    public void testFindFirstIsOptionalAndShortCircuits()
    {
        Capture<Integer> capture = newCapture();
        AtomicInteger reads = new AtomicInteger();
        MatchCursor cursor = values(capture, new AtomicInteger(), reads).matchCursor(9);
        assertThat(cursor.findFirst()).map(captures -> captures.get(capture)).contains(1);
        assertThat(reads.get()).isEqualTo(1);
        assertThat(cursor.findFirst()).map(captures -> captures.get(capture)).contains(2);
        assertThat(reads.get()).isEqualTo(2);
        assertThat(cursor.findFirst()).isEmpty();
        assertThat(MatchCursor.empty().findFirst()).isEmpty();
        assertThatThrownBy(MatchCursor.empty()::current).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSingleCaptures()
    {
        Capture<Integer> capture = newCapture();
        Captures captures = Captures.ofNullable(capture, 7);
        MatchCursor cursor = MatchCursor.single(captures);
        assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        assertThat(cursor.findFirst()).containsSame(captures);
        assertThat(cursor.current()).isSameAs(captures);
        assertThat(cursor.matches()).isFalse();
        assertThat(cursor.findFirst()).isEmpty();
        assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        assertThat(captures.get(capture)).isEqualTo(7);
    }

    private static Pattern<Integer> values(Capture<Integer> capture, AtomicInteger opens, AtomicInteger reads)
    {
        return new CustomPattern<>(Optional.empty())
        {
            @Override
            public <C> MatchCursor accept(Object object, Captures captures, C context)
            {
                opens.incrementAndGet();
                return Pattern.any().or(
                                p -> p.with(Property.property("first", (Object _) -> {
                                    reads.incrementAndGet();
                                    return 1;
                                }).capturedAs(capture)),
                                p -> p.with(Property.property("second", (Object _) -> {
                                    reads.incrementAndGet();
                                    return 2;
                                }).capturedAs(capture)))
                        .matchCursor(object, captures, context);
            }

            @Override
            public String print()
            {
                return "values";
            }
        };
    }
}
