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
package io.trino.lance.file.v2.reader;

import io.trino.lance.file.v2.reader.MiniBlockPageReader.PreambleAction;
import io.trino.lance.file.v2.reader.MiniBlockPageReader.SelectedRanges;
import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.lance.file.v2.reader.MiniBlockPageReader.mapRange;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMiniblockMapRange
{
    @Test
    void testMiddleNull()
    {
        // NULL in the middle
        // [[A, B, C], [D, E], NULL, [F, G, H]]
        int[] repetitions = new int[] {1, 0, 0, 1, 0, 1, 1, 0, 0};
        int[] definitions = new int[] {0, 0, 0, 0, 0, 1, 0, 0, 0};
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 8;
        Function<Range, SelectedRanges> calculate = range -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, PreambleAction.ABSENT);

        assertThat(calculate.apply(new Range(0, 1))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 3)));
        assertThat(calculate.apply(new Range(1, 2))).isEqualTo(new SelectedRanges(new Range(3, 5), new Range(3, 5)));
        assertThat(calculate.apply(new Range(2, 3))).isEqualTo(new SelectedRanges(new Range(5, 5), new Range(5, 6)));
        assertThat(calculate.apply(new Range(3, 4))).isEqualTo(new SelectedRanges(new Range(5, 8), new Range(6, 9)));
        assertThat(calculate.apply(new Range(3, 4))).isEqualTo(new SelectedRanges(new Range(5, 8), new Range(6, 9)));
        assertThat(calculate.apply(new Range(0, 2))).isEqualTo(new SelectedRanges(new Range(0, 5), new Range(0, 5)));
        assertThat(calculate.apply(new Range(0, 2))).isEqualTo(new SelectedRanges(new Range(0, 5), new Range(0, 5)));
        assertThat(calculate.apply(new Range(1, 3))).isEqualTo(new SelectedRanges(new Range(3, 5), new Range(3, 6)));
        assertThat(calculate.apply(new Range(2, 4))).isEqualTo(new SelectedRanges(new Range(5, 8), new Range(5, 9)));
        assertThat(calculate.apply(new Range(0, 3))).isEqualTo(new SelectedRanges(new Range(0, 5), new Range(0, 6)));
        assertThat(calculate.apply(new Range(1, 4))).isEqualTo(new SelectedRanges(new Range(3, 8), new Range(3, 9)));
        assertThat(calculate.apply(new Range(0, 4))).isEqualTo(new SelectedRanges(new Range(0, 8), new Range(0, 9)));
    }

    @Test
    void testLeadingNull()
    {
        // NULL at the begining
        // [NULL, [A, B], [C]]
        int[] repetitions = new int[] {1, 1, 0, 1};
        int[] definitions = new int[] {1, 0, 0, 0};
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 3;
        Function<Range, SelectedRanges> calculate = range -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, PreambleAction.ABSENT);

        assertThat(calculate.apply(new Range(0, 1))).isEqualTo(new SelectedRanges(new Range(0, 0), new Range(0, 1)));
        assertThat(calculate.apply(new Range(1, 2))).isEqualTo(new SelectedRanges(new Range(0, 2), new Range(1, 3)));
        assertThat(calculate.apply(new Range(2, 3))).isEqualTo(new SelectedRanges(new Range(2, 3), new Range(3, 4)));
        assertThat(calculate.apply(new Range(0, 2))).isEqualTo(new SelectedRanges(new Range(0, 2), new Range(0, 3)));
        assertThat(calculate.apply(new Range(1, 3))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(1, 4)));
        assertThat(calculate.apply(new Range(0, 3))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 4)));
    }

    @Test
    void testTrailingNull()
    {
        // Null at end
        // [[A], [B, C], NULL]
        int[] repetitions = new int[] {1, 1, 0, 1};
        int[] definitions = new int[] {0, 0, 0, 1};
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 3;
        Function<Range, SelectedRanges> calculate = range -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, PreambleAction.ABSENT);

        assertThat(calculate.apply(new Range(1, 2))).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(1, 3)));
        assertThat(calculate.apply(new Range(2, 3))).isEqualTo(new SelectedRanges(new Range(3, 3), new Range(3, 4)));
        assertThat(calculate.apply(new Range(0, 2))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 3)));
        assertThat(calculate.apply(new Range(1, 3))).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(1, 4)));
        assertThat(calculate.apply(new Range(0, 3))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 4)));
    }

    @Test
    void testNoNulls()
    {
        // No nulls, with repetition
        // [[A, B], [C, D], [E, F]]
        int[] repetitions = new int[] {1, 0, 1, 0, 1, 0};
        int[] definitions = null;
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 6;
        Function<Range, SelectedRanges> calculate = range -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, PreambleAction.ABSENT);

        assertThat(calculate.apply(new Range(0, 1))).isEqualTo(new SelectedRanges(new Range(0, 2), new Range(0, 2)));
        assertThat(calculate.apply(new Range(1, 2))).isEqualTo(new SelectedRanges(new Range(2, 4), new Range(2, 4)));
        assertThat(calculate.apply(new Range(2, 3))).isEqualTo(new SelectedRanges(new Range(4, 6), new Range(4, 6)));
        assertThat(calculate.apply(new Range(0, 2))).isEqualTo(new SelectedRanges(new Range(0, 4), new Range(0, 4)));
        assertThat(calculate.apply(new Range(1, 3))).isEqualTo(new SelectedRanges(new Range(2, 6), new Range(2, 6)));
        assertThat(calculate.apply(new Range(0, 3))).isEqualTo(new SelectedRanges(new Range(0, 6), new Range(0, 6)));
    }

    @Test
    void testNoRepetitions()
    {
        // No repetition, with nulls
        // [A, B, NULL, C]
        int[] repetitions = null;
        int[] definitions = new int[] {0, 0, 1, 0};
        int maxVisibleDef = 1;
        int maxRepetitionLevel = 1;
        int totalItems = 4;
        Function<Range, SelectedRanges> calculate = range -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, PreambleAction.ABSENT);

        assertThat(calculate.apply(new Range(0, 1))).isEqualTo(new SelectedRanges(new Range(0, 1), new Range(0, 1)));
        assertThat(calculate.apply(new Range(1, 2))).isEqualTo(new SelectedRanges(new Range(1, 2), new Range(1, 2)));
        assertThat(calculate.apply(new Range(2, 3))).isEqualTo(new SelectedRanges(new Range(2, 3), new Range(2, 3)));
        assertThat(calculate.apply(new Range(0, 2))).isEqualTo(new SelectedRanges(new Range(0, 2), new Range(0, 2)));
        assertThat(calculate.apply(new Range(1, 3))).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(1, 3)));
        assertThat(calculate.apply(new Range(0, 3))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 3)));
    }

    @Test
    void testTrailingNullWithPreamble()
    {
        // [[..., A] [B, C], NULL]
        int[] repetitions = new int[] {0, 1, 0, 1};
        int[] definitions = new int[] {0, 0, 0, 1};
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 3;
        BiFunction<Range, PreambleAction, SelectedRanges> calculate = (range, preambleAction) -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, preambleAction);

        assertThat(calculate.apply(new Range(0, 1), PreambleAction.TAKE)).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 3)));
        assertThat(calculate.apply(new Range(0, 2), PreambleAction.TAKE)).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 4)));
        assertThat(calculate.apply(new Range(1, 2), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(3, 3), new Range(3, 4)));
        assertThat(calculate.apply(new Range(0, 2), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(1, 4)));
    }

    @Test
    void testPreambleWithMiddleNull()
    {
        // [[..., A], NULL, [D, E]]
        int[] repetitions = new int[] {0, 1, 1, 0};
        int[] definitions = new int[] {0, 1, 0, 0};
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 4;
        BiFunction<Range, PreambleAction, SelectedRanges> calculate = (range, preambleAction) -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, preambleAction);

        assertThat(calculate.apply(new Range(0, 1), PreambleAction.TAKE)).isEqualTo(new SelectedRanges(new Range(0, 1), new Range(0, 2)));
        assertThat(calculate.apply(new Range(0, 2), PreambleAction.TAKE)).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 4)));
        assertThat(calculate.apply(new Range(0, 1), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(1, 1), new Range(1, 2)));
        assertThat(calculate.apply(new Range(1, 2), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(2, 4)));
        assertThat(calculate.apply(new Range(0, 2), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(1, 4)));
    }

    @Test
    void testPreambleWithoutDefinition()
    {
        // [[..., A] [B, C], [D]]
        int[] repetitions = new int[] {0, 1, 0, 1};
        int[] definitions = null;
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 4;
        BiFunction<Range, PreambleAction, SelectedRanges> calculate = (range, preambleAction) -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, preambleAction);

        assertThat(calculate.apply(new Range(0, 1), PreambleAction.TAKE)).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 3)));
        assertThat(calculate.apply(new Range(0, 2), PreambleAction.TAKE)).isEqualTo(new SelectedRanges(new Range(0, 4), new Range(0, 4)));
        assertThat(calculate.apply(new Range(0, 1), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(1, 3)));
        assertThat(calculate.apply(new Range(1, 2), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(3, 4), new Range(3, 4)));
        assertThat(calculate.apply(new Range(0, 2), PreambleAction.SKIP)).isEqualTo(new SelectedRanges(new Range(1, 4), new Range(1, 4)));
    }

    @Test
    void testEmptyList()
    {
        // [[] [A], [B, C]]
        int[] repetitions = new int[] {1, 1, 1, 0};
        int[] definitions = new int[] {1, 0, 0, 0};
        int maxVisibleDef = 0;
        int maxRepetitionLevel = 1;
        int totalItems = 3;
        Function<Range, SelectedRanges> calculate = (range) -> mapRange(range, repetitions, definitions, maxRepetitionLevel, maxVisibleDef, totalItems, PreambleAction.ABSENT);

        assertThat(calculate.apply(new Range(0, 1))).isEqualTo(new SelectedRanges(new Range(0, 0), new Range(0, 1)));
        assertThat(calculate.apply(new Range(1, 2))).isEqualTo(new SelectedRanges(new Range(0, 1), new Range(1, 2)));
        assertThat(calculate.apply(new Range(2, 3))).isEqualTo(new SelectedRanges(new Range(1, 3), new Range(2, 4)));
        assertThat(calculate.apply(new Range(0, 2))).isEqualTo(new SelectedRanges(new Range(0, 1), new Range(0, 2)));
        assertThat(calculate.apply(new Range(1, 3))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(1, 4)));
        assertThat(calculate.apply(new Range(0, 3))).isEqualTo(new SelectedRanges(new Range(0, 3), new Range(0, 4)));
    }
}
