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
package io.trino.operator.window.matcher;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import org.assertj.core.api.AssertProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.sql.planner.rowpattern.Patterns.alternation;
import static io.trino.sql.planner.rowpattern.Patterns.concatenation;
import static io.trino.sql.planner.rowpattern.Patterns.empty;
import static io.trino.sql.planner.rowpattern.Patterns.end;
import static io.trino.sql.planner.rowpattern.Patterns.excluded;
import static io.trino.sql.planner.rowpattern.Patterns.label;
import static io.trino.sql.planner.rowpattern.Patterns.permutation;
import static io.trino.sql.planner.rowpattern.Patterns.plusQuantified;
import static io.trino.sql.planner.rowpattern.Patterns.questionMarkQuantified;
import static io.trino.sql.planner.rowpattern.Patterns.starQuantified;
import static io.trino.sql.planner.rowpattern.Patterns.start;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPatternMatchingMachine
{
    private static final Map<IrLabel, Integer> LABEL_MAPPING = ImmutableMap.of(
            new IrLabel("A"), 0,
            new IrLabel("B"), 1,
            new IrLabel("C"), 2,
            new IrLabel("D"), 3,
            new IrLabel("E"), 4);

    @Test
    public void testCaptures()
    {
        assertThat(match(
                concatenation(excluded(label("A")), label("B")),
                "ABCD"))
                .hasCaptures(new int[] {0, 1});

        assertThat(match(
                excluded(concatenation(label("A"), label("B"))),
                "ABCD"))
                .hasCaptures(new int[] {0, 2});

        assertThat(match(
                concatenation(label("A"), excluded(label("B"))),
                "ABCD"))
                .hasCaptures(new int[] {1, 2});

        assertThat(match(
                concatenation(label("A"), starQuantified(excluded(label("B")), true)),
                "ABBBCD"))
                .hasCaptures(new int[] {1, 2, 2, 3, 3, 4});

        assertThat(match(
                concatenation(label("A"), excluded(starQuantified(label("B"), true))),
                "ABBBCD"))
                .hasCaptures(new int[] {1, 4});

        assertThat(match(
                concatenation(label("A"), starQuantified(excluded(label("B")), true), label("C"), excluded(starQuantified(label("D"), true))),
                "ABBCDDDE"))
                .hasCaptures(new int[] {1, 2, 2, 3, 4, 7});

        assertThat(match(
                concatenation(label("A"), starQuantified(concatenation(excluded(concatenation(label("B"), label("C"))), label("D"), excluded(label("E"))), true)),
                "ABCDEBCDEBCDE"))
                .hasCaptures(new int[] {1, 3, 4, 5, 5, 7, 8, 9, 9, 11, 12, 13});
    }

    @Test
    public void testMatchWithoutCaptures()
    {
        assertThat(match(
                concatenation(label("A"), label("B")),
                "ABCD"))
                .hasCaptures(new int[] {});
    }

    @Test
    public void testEmptyMatch()
    {
        assertThat(match(empty(), "ABCD"))
                .hasCaptures(new int[] {})
                .hasLabels(new char[] {});
    }

    @Test
    public void testNoMatch()
    {
        assertThat(match(label("A"), "B"))
                .isNoMatch();
    }

    @Test
    public void testLabels()
    {
        assertThat(match(
                concatenation(label("A"), label("B")),
                "ABCD"))
                .hasLabels(new char[] {'A', 'B'});

        assertThat(match(
                concatenation(starQuantified(label("A"), true), label("B")),
                "AABCD"))
                .hasLabels(new char[] {'A', 'A', 'B'});

        assertThat(match(
                concatenation(starQuantified(label("A"), true), label("B")),
                "BCD"))
                .hasLabels(new char[] {'B'});

        assertThat(match(
                concatenation(start(), label("A"), label("B")),
                "ABCD"))
                .hasLabels(new char[] {'A', 'B'});

        assertThat(match(
                concatenation(label("A"), label("B"), end()),
                "AB"))
                .hasLabels(new char[] {'A', 'B'});

        assertThat(match(
                concatenation(excluded(label("A")), label("B")),
                "ABCD"))
                .hasLabels(new char[] {'A', 'B'});

        assertThat(match(
                alternation(concatenation(label("A"), label("B")), concatenation(label("B"), label("C"))),
                "ABCD"))
                .hasLabels(new char[] {'A', 'B'});

        assertThat(match(
                permutation(label("A"), label("B"), label("C")),
                "ABCD"))
                .hasLabels(new char[] {'A', 'B', 'C'});

        assertThat(match(
                concatenation(label("A"), questionMarkQuantified(label("B"), true)),
                "ABCD"))
                .hasLabels(new char[] {'A', 'B'});

        assertThat(match(
                concatenation(label("A"), questionMarkQuantified(label("B"), false)),
                "ABCD"))
                .hasLabels(new char[] {'A'});
    }

    @Test
    public void testLongMatch()
    {
        assertThat(match(
                starQuantified(label("A"), true),
                "AAAAAAAAAA"))
                .hasLabels(new char[] {'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A'});

        assertThat(match(
                concatenation(start(), plusQuantified(plusQuantified(label("A"), true), true), end()),
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"))
                .isNoMatch();
    }

    private static AssertProvider<MatchAssert> match(IrRowPattern pattern, String input)
    {
        return MatchAssert.match(pattern, input, LABEL_MAPPING);
    }
}
