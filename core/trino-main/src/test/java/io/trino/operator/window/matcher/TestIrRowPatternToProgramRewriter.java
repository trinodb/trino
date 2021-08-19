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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
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
import static org.testng.Assert.assertEquals;

public class TestIrRowPatternToProgramRewriter
{
    private static final Map<IrLabel, Integer> LABEL_MAPPING = ImmutableMap.of(
            new IrLabel("A"), 0,
            new IrLabel("B"), 1,
            new IrLabel("C"), 2,
            new IrLabel("D"), 3,
            new IrLabel("E"), 4);

    @Test
    public void testPatternLabel()
    {
        assertRewritten(label("A"), ImmutableList.of(new MatchLabel(0), new Done()));
    }

    @Test
    public void testEmptyPattern()
    {
        assertRewritten(empty(), ImmutableList.of(new Done()));
    }

    @Test
    public void testAnchorPattern()
    {
        assertRewritten(start(), ImmutableList.of(new MatchStart(), new Done()));
        assertRewritten(end(), ImmutableList.of(new MatchEnd(), new Done()));
    }

    @Test
    public void testPatternExclusion()
    {
        assertRewritten(excluded(label("A")), ImmutableList.of(new Save(), new MatchLabel(0), new Save(), new Done()));
    }

    @Test
    public void testPatternAlternation()
    {
        assertRewritten(alternation(label("A"), label("B")), ImmutableList.of(
                new Split(1, 3),        // 0
                new MatchLabel(0),      // 1
                new Jump(4),     // 2
                new MatchLabel(1),      // 3
                new Done()));           // 4

        assertRewritten(alternation(label("A"), label("B"), label("C"), label("D")), ImmutableList.of(
                new Split(1, 3),        // 0
                new MatchLabel(0),      // 1
                new Jump(10),    // 2
                new Split(4, 6),        // 3
                new MatchLabel(1),      // 4
                new Jump(10),    // 5
                new Split(7, 9),        // 6
                new MatchLabel(2),      // 7
                new Jump(10),    // 8
                new MatchLabel(3),      // 9
                new Done()));           // 10
    }

    @Test
    public void testPatternConcatenation()
    {
        assertRewritten(concatenation(label("A"), label("B")), ImmutableList.of(new MatchLabel(0), new MatchLabel(1), new Done()));
        assertRewritten(concatenation(label("A"), label("B"), label("C"), label("D")), ImmutableList.of(new MatchLabel(0), new MatchLabel(1), new MatchLabel(2), new MatchLabel(3), new Done()));
    }

    @Test
    public void testPatternPermutation()
    {
        assertRewritten(permutation(label("A"), label("B")), ImmutableList.of(
                new Split(1, 4),        // 0
                new MatchLabel(0),      // 1
                new MatchLabel(1),      // 2
                new Jump(6),     // 3
                new MatchLabel(1),      // 4
                new MatchLabel(0),      // 5
                new Done()));           // 6

        assertRewritten(permutation(label("A"), label("B"), label("C")), ImmutableList.of(
                new Split(1, 5),        // 0
                new MatchLabel(0),      // 1
                new MatchLabel(1),      // 2
                new MatchLabel(2),      // 3
                new Jump(28),    // 4
                new Split(6, 10),       // 5
                new MatchLabel(0),      // 6
                new MatchLabel(2),      // 7
                new MatchLabel(1),      // 8
                new Jump(28),    // 9
                new Split(11, 15),      // 10
                new MatchLabel(1),      // 11
                new MatchLabel(0),      // 12
                new MatchLabel(2),      // 13
                new Jump(28),    // 14
                new Split(16, 20),      // 15
                new MatchLabel(1),      // 16
                new MatchLabel(2),      // 17
                new MatchLabel(0),      // 18
                new Jump(28),    // 19
                new Split(21, 25),      // 20
                new MatchLabel(2),      // 21
                new MatchLabel(0),      // 22
                new MatchLabel(1),      // 23
                new Jump(28),    // 24
                new MatchLabel(2),      // 25
                new MatchLabel(1),      // 26
                new MatchLabel(0),      // 27
                new Done()));           // 28
    }

    @Test
    public void testQuantifiers()
    {
        assertRewritten(starQuantified(label("A"), true), ImmutableList.of(
                new Split(1, 3),        // 0
                new MatchLabel(0),      // 1
                new Split(1, 3),        // 2
                new Done()));           // 3

        assertRewritten(starQuantified(label("A"), false), ImmutableList.of(
                new Split(3, 1),        // 0
                new MatchLabel(0),      // 1
                new Split(3, 1),        // 2
                new Done()));           // 3

        assertRewritten(plusQuantified(label("A"), true), ImmutableList.of(
                new MatchLabel(0),      // 0
                new Split(0, 2),        // 1
                new Done()));           // 2

        assertRewritten(plusQuantified(label("A"), false), ImmutableList.of(
                new MatchLabel(0),      // 0
                new Split(2, 0),        // 1
                new Done()));           // 2

        assertRewritten(questionMarkQuantified(label("A"), true), ImmutableList.of(
                new Split(1, 2),        // 0
                new MatchLabel(0),      // 1
                new Done()));           // 2

        assertRewritten(questionMarkQuantified(label("A"), false), ImmutableList.of(
                new Split(2, 1),        // 0
                new MatchLabel(0),      // 1
                new Done()));           // 2

        assertRewritten(
                rangeQuantified(label("A"), 0, Optional.empty(), true),
                ImmutableList.of(
                        new Split(1, 3),        // 0
                        new MatchLabel(0),      // 1
                        new Split(1, 3),        // 2
                        new Done()));           // 3

        assertRewritten(
                rangeQuantified(label("A"), 0, Optional.empty(), false),
                ImmutableList.of(
                        new Split(3, 1),        // 0
                        new MatchLabel(0),      // 1
                        new Split(3, 1),        // 2
                        new Done()));           // 3

        assertRewritten(
                rangeQuantified(label("A"), 3, Optional.empty(), true),
                ImmutableList.of(
                        new MatchLabel(0),      // 0
                        new MatchLabel(0),      // 1
                        new MatchLabel(0),      // 2
                        new Split(2, 4),        // 3
                        new Done()));           // 4

        assertRewritten(
                rangeQuantified(label("A"), 3, Optional.empty(), false),
                ImmutableList.of(
                        new MatchLabel(0),      // 0
                        new MatchLabel(0),      // 1
                        new MatchLabel(0),      // 2
                        new Split(4, 2),        // 3
                        new Done()));           // 4

        assertRewritten(
                rangeQuantified(label("A"), 0, Optional.of(3), true),
                ImmutableList.of(
                        new Split(1, 6),        // 0
                        new MatchLabel(0),      // 1
                        new Split(3, 6),        // 2
                        new MatchLabel(0),      // 3
                        new Split(5, 6),        // 4
                        new MatchLabel(0),      // 5
                        new Done()));           // 6

        assertRewritten(
                rangeQuantified(label("A"), 0, Optional.of(3), false),
                ImmutableList.of(
                        new Split(6, 1),        // 0
                        new MatchLabel(0),      // 1
                        new Split(6, 3),        // 2
                        new MatchLabel(0),      // 3
                        new Split(6, 5),        // 4
                        new MatchLabel(0),      // 5
                        new Done()));           // 6

        assertRewritten(
                rangeQuantified(label("A"), 3, Optional.of(3), true),
                ImmutableList.of(
                        new MatchLabel(0),      // 0
                        new MatchLabel(0),      // 1
                        new MatchLabel(0),      // 2
                        new Done()));           // 3

        assertRewritten(
                rangeQuantified(label("A"), 3, Optional.of(3), false),
                ImmutableList.of(
                        new MatchLabel(0),      // 0
                        new MatchLabel(0),      // 1
                        new MatchLabel(0),      // 2
                        new Done()));           // 3

        assertRewritten(
                rangeQuantified(label("A"), 0, Optional.of(0), true),
                ImmutableList.of(new Done()));

        assertRewritten(
                rangeQuantified(label("A"), 0, Optional.of(0), false),
                ImmutableList.of(new Done()));

        assertRewritten(
                rangeQuantified(label("A"), 2, Optional.of(4), true),
                ImmutableList.of(
                        new MatchLabel(0),      // 0
                        new MatchLabel(0),      // 1
                        new Split(3, 6),        // 2
                        new MatchLabel(0),      // 3
                        new Split(5, 6),        // 4
                        new MatchLabel(0),      // 5
                        new Done()));           // 6

        assertRewritten(
                rangeQuantified(label("A"), 2, Optional.of(4), false),
                ImmutableList.of(
                        new MatchLabel(0),      // 0
                        new MatchLabel(0),      // 1
                        new Split(6, 3),        // 2
                        new MatchLabel(0),      // 3
                        new Split(6, 5),        // 4
                        new MatchLabel(0),      // 5
                        new Done()));           // 6
    }

    private void assertRewritten(IrRowPattern pattern, List<Instruction> expected)
    {
        Program program = IrRowPatternToProgramRewriter.rewrite(pattern, LABEL_MAPPING);

        assertEquals(program.getInstructions(), expected);
    }
}
