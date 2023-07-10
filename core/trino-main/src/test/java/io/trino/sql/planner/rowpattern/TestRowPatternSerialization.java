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
package io.trino.sql.planner.rowpattern;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.sql.planner.rowpattern.ir.IrQuantifier;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.rowpattern.Patterns.alternation;
import static io.trino.sql.planner.rowpattern.Patterns.concatenation;
import static io.trino.sql.planner.rowpattern.Patterns.empty;
import static io.trino.sql.planner.rowpattern.Patterns.end;
import static io.trino.sql.planner.rowpattern.Patterns.excluded;
import static io.trino.sql.planner.rowpattern.Patterns.label;
import static io.trino.sql.planner.rowpattern.Patterns.permutation;
import static io.trino.sql.planner.rowpattern.Patterns.plusQuantified;
import static io.trino.sql.planner.rowpattern.Patterns.questionMarkQuantified;
import static io.trino.sql.planner.rowpattern.Patterns.rangeQuantified;
import static io.trino.sql.planner.rowpattern.Patterns.starQuantified;
import static io.trino.sql.planner.rowpattern.Patterns.start;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.oneOrMore;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.range;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.zeroOrMore;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.zeroOrOne;
import static org.testng.Assert.assertEquals;

public class TestRowPatternSerialization
{
    @Test
    public void testPatternQuantifierRoundtrip()
    {
        JsonCodec<IrQuantifier> codec = new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(IrQuantifier.class);

        assertJsonRoundTrip(codec, zeroOrMore(true));
        assertJsonRoundTrip(codec, zeroOrMore(false));

        assertJsonRoundTrip(codec, oneOrMore(true));
        assertJsonRoundTrip(codec, oneOrMore(false));

        assertJsonRoundTrip(codec, zeroOrOne(true));
        assertJsonRoundTrip(codec, zeroOrOne(false));

        assertJsonRoundTrip(codec, range(Optional.empty(), Optional.empty(), true));
        assertJsonRoundTrip(codec, range(Optional.empty(), Optional.empty(), false));
        assertJsonRoundTrip(codec, range(Optional.of(5), Optional.empty(), true));
        assertJsonRoundTrip(codec, range(Optional.of(5), Optional.empty(), false));
        assertJsonRoundTrip(codec, range(Optional.empty(), Optional.of(5), true));
        assertJsonRoundTrip(codec, range(Optional.empty(), Optional.of(5), false));
        assertJsonRoundTrip(codec, range(Optional.of(5), Optional.of(10), true));
        assertJsonRoundTrip(codec, range(Optional.of(5), Optional.of(10), false));
    }

    @Test
    public void testRowPatternRoundtrip()
    {
        JsonCodec<IrRowPattern> codec = new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(IrRowPattern.class);

        assertJsonRoundTrip(codec, start());
        assertJsonRoundTrip(codec, end());

        assertJsonRoundTrip(codec, empty());

        assertJsonRoundTrip(codec, excluded(empty()));

        assertJsonRoundTrip(codec, label("name"));
        assertJsonRoundTrip(codec, label(""));
        assertJsonRoundTrip(codec, label("^"));
        assertJsonRoundTrip(codec, label("$"));

        assertJsonRoundTrip(codec, alternation(empty(), empty(), empty()));

        assertJsonRoundTrip(codec, concatenation(empty(), empty(), empty()));

        assertJsonRoundTrip(codec, permutation(empty(), empty(), empty()));

        assertJsonRoundTrip(codec, starQuantified(empty(), true));

        assertJsonRoundTrip(
                codec,
                concatenation(
                        alternation(
                                starQuantified(start(), true),
                                plusQuantified(end(), false),
                                questionMarkQuantified(empty(), true)),
                        concatenation(
                                excluded(rangeQuantified(empty(), 0, Optional.empty(), true)),
                                rangeQuantified(label("name_0"), 5, Optional.empty(), false),
                                label("name_1")),
                        permutation(
                                alternation(label("name_2"), rangeQuantified(label("name_3"), 0, Optional.of(5), true)),
                                rangeQuantified(concatenation(label("name_4"), label("name_5")), 5, Optional.of(10), false))));
    }

    public static <T> void assertJsonRoundTrip(JsonCodec<T> codec, T object)
    {
        String json = codec.toJson(object);
        T copy = codec.fromJson(json);
        assertEquals(copy, object);
    }
}
