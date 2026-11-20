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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortingProperty;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSymbolEquivalences
{
    private static final Symbol A = new Symbol(BIGINT, "a");
    private static final Symbol B = new Symbol(BIGINT, "b");
    private static final Symbol C = new Symbol(BIGINT, "c");
    private static final Symbol V = new Symbol(VARCHAR, "v");

    @Test
    public void testGroupingSatisfiedThroughEquivalentSymbol()
    {
        SymbolEquivalences equivalences = SymbolEquivalences.empty().with(A, B);

        assertThat(LocalProperties.match(ImmutableList.of(grouped(A)), ImmutableList.of(grouped(B)), equivalences))
                .containsExactly(Optional.empty());
        assertThat(LocalProperties.match(ImmutableList.of(grouped(A)), ImmutableList.of(grouped(B)), SymbolEquivalences.empty()))
                .containsExactly(Optional.of(grouped(B)));
    }

    @Test
    public void testSortingSatisfiedThroughEquivalentSymbol()
    {
        SymbolEquivalences equivalences = SymbolEquivalences.empty().with(A, B);

        assertThat(LocalProperties.match(ImmutableList.of(sorted(A)), ImmutableList.of(sorted(B), sorted(A)), equivalences))
                .containsExactly(Optional.empty(), Optional.empty());
        assertThat(LocalProperties.match(ImmutableList.of(sorted(A)), ImmutableList.of(sorted(B), sorted(C)), equivalences))
                .containsExactly(Optional.empty(), Optional.of(sorted(C)));
    }

    @Test
    public void testUnsatisfiedPropertyReportsDesiredSymbols()
    {
        // grouped on a only satisfies the a/b part of the desired grouping; callers use the unsatisfied columns
        // to decide which of their own symbols are not pre-grouped, so they must be their symbols and not representatives
        SymbolEquivalences equivalences = SymbolEquivalences.empty().with(A, B);

        List<Optional<LocalProperty<Symbol>>> result = LocalProperties.match(
                ImmutableList.of(grouped(A)),
                ImmutableList.of(grouped(B, C)),
                equivalences);
        assertThat(result).containsExactly(Optional.of(grouped(C)));

        result = LocalProperties.match(
                ImmutableList.of(grouped(C)),
                ImmutableList.of(grouped(B, A)),
                equivalences);
        assertThat(result).containsExactly(Optional.of(grouped(B, A)));
    }

    @Test
    public void testEquivalencesAreMergedTransitively()
    {
        SymbolEquivalences equivalences = SymbolEquivalences.empty().with(A, B).with(B, C);

        assertThat(equivalences.getClasses()).containsExactly(ImmutableSet.of(A, B, C));
        assertThat(equivalences.representative(C)).isEqualTo(A);
        assertThat(LocalProperties.match(ImmutableList.of(sorted(C)), ImmutableList.of(sorted(A)), equivalences))
                .containsExactly(Optional.empty());
    }

    @Test
    public void testTranslateDropsClassesLeftWithOneSymbol()
    {
        SymbolEquivalences equivalences = SymbolEquivalences.empty().with(A, B).with(C, new Symbol(BIGINT, "d"));

        SymbolEquivalences translated = equivalences.translate(symbol -> symbol.equals(B) || symbol.equals(C) ? Optional.of(symbol) : Optional.empty());
        assertThat(translated.isEmpty()).isTrue();
    }

    @Test
    public void testSymbolsOfDifferentTypesAreNotEquated()
    {
        assertThat(SymbolEquivalences.empty().with(A, V).isEmpty()).isTrue();
    }

    @Test
    public void testTypesWithInconsistentEqualityAreNotEquated()
    {
        assertThat(SymbolEquivalences.empty().with(new Symbol(DOUBLE, "x"), new Symbol(DOUBLE, "y")).isEmpty()).isTrue();
        assertThat(SymbolEquivalences.empty().with(new Symbol(TIMESTAMP_TZ_MILLIS, "x"), new Symbol(TIMESTAMP_TZ_MILLIS, "y")).isEmpty()).isTrue();
    }

    private static LocalProperty<Symbol> grouped(Symbol... symbols)
    {
        return new GroupingProperty<>(ImmutableList.copyOf(symbols));
    }

    private static LocalProperty<Symbol> sorted(Symbol symbol)
    {
        return new SortingProperty<>(symbol, ASC_NULLS_FIRST);
    }
}
