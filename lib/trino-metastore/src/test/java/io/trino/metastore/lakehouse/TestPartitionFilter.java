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
package io.trino.metastore.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.metastore.Column;
import io.trino.metastore.HiveType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metastore.Partitions.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPartitionFilter
{
    // The filter built from a Trino predicate

    @Test
    void pushesDownASingleStringValue()
    {
        assertThat(PartitionFilter.toLakehouseFilter(List.of("region"), singleValue("region", VARCHAR, "emea")))
                .isEqualTo("region='emea'");
    }

    @Test
    void pushesDownACharAndABoundedVarcharValue()
    {
        assertThat(PartitionFilter.toLakehouseFilter(List.of("region"), singleValue("region", createCharType(4), "emea")))
                .isEqualTo("region='emea'");
        assertThat(PartitionFilter.toLakehouseFilter(List.of("region"), singleValue("region", createVarcharType(10), "emea")))
                .isEqualTo("region='emea'");
    }

    @Test
    void joinsTheConditionsInTheOrderOfTheColumns()
    {
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                "region", Domain.singleValue(VARCHAR, utf8Slice("emea")),
                "year", Domain.singleValue(VARCHAR, utf8Slice("2026"))));

        assertThat(PartitionFilter.toLakehouseFilter(List.of("year", "region"), predicate))
                .isEqualTo("year='2026' and region='emea'");
    }

    @Test
    void leavesOutAColumnWithoutADomainAndADomainWithoutAColumn()
    {
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                "year", Domain.singleValue(VARCHAR, utf8Slice("2026")),
                "other", Domain.singleValue(VARCHAR, utf8Slice("x"))));

        assertThat(PartitionFilter.toLakehouseFilter(List.of("region", "year"), predicate))
                .isEqualTo("year='2026'");
    }

    @Test
    void pushesNothingDownForARangeASetOrANull()
    {
        Slice emea = utf8Slice("emea");
        List<Domain> domains = List.of(
                Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, emea)), false),
                Domain.multipleValues(VARCHAR, List.of(emea, utf8Slice("apac"))),
                Domain.onlyNull(VARCHAR));

        for (Domain domain : domains) {
            TupleDomain<String> predicate = TupleDomain.withColumnDomains(Map.of("region", domain));

            assertThat(PartitionFilter.toLakehouseFilter(List.of("region"), predicate)).isEmpty();
        }
    }

    @Test
    void pushesNothingDownForANonStringType()
    {
        assertThat(PartitionFilter.toLakehouseFilter(List.of("year"), singleValue("year", INTEGER, 2026L))).isEmpty();
    }

    @Test
    void leavesOutAValueWithAQuoteAndKeepsTheRest()
    {
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                "region", Domain.singleValue(VARCHAR, utf8Slice("o'brien")),
                "year", Domain.singleValue(VARCHAR, utf8Slice("2026"))));

        assertThat(PartitionFilter.toLakehouseFilter(List.of("region", "year"), predicate))
                .isEqualTo("year='2026'");
    }

    @Test
    void buildsAnEmptyFilterForAPredicateThatAcceptsEverything()
    {
        assertThat(PartitionFilter.toLakehouseFilter(List.of("region"), TupleDomain.all())).isEmpty();
    }

    @Test
    void refusesAPredicateThatMatchesNothing()
    {
        assertThatThrownBy(() -> PartitionFilter.toLakehouseFilter(List.of("region"), TupleDomain.none()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("matches nothing");
    }

    // The filter built from the values of one partition

    @Test
    void writesIntegerTypesAsNumbers()
    {
        for (String type : List.of("tinyint", "smallint", "int", "bigint")) {
            assertThat(PartitionFilter.toLakehouseFilter(List.of(column("n", type)), List.of("42")))
                    .isEqualTo("n=42");
        }
    }

    @Test
    void writesDateAndTimestampValuesAfterACast()
    {
        assertThat(PartitionFilter.toLakehouseFilter(List.of(column("d", "date")), List.of("2026-01-31")))
                .isEqualTo("d=CAST('2026-01-31' as date)");
        assertThat(PartitionFilter.toLakehouseFilter(List.of(column("t", "timestamp")), List.of("2026-01-31 10:15:30")))
                .isEqualTo("t=CAST('2026-01-31 10:15:30' as timestamp)");
    }

    @Test
    void writesEveryOtherTypeAsAString()
    {
        for (String type : List.of("string", "varchar(20)", "char(4)", "boolean", "decimal(10,2)")) {
            assertThat(PartitionFilter.toLakehouseFilter(List.of(column("c", type)), List.of("v")))
                    .isEqualTo("c='v'");
        }
    }

    @Test
    void writesTheDefaultPartitionAsANullCheck()
    {
        assertThat(PartitionFilter.toLakehouseFilter(List.of(column("region", "string")), List.of(HIVE_DEFAULT_DYNAMIC_PARTITION)))
                .isEqualTo("region is null");
        assertThat(PartitionFilter.toLakehouseFilter(List.of(column("year", "int")), List.of(HIVE_DEFAULT_DYNAMIC_PARTITION)))
                .isEqualTo("year is null");
    }

    @Test
    void joinsOneConditionPerColumnInOrder()
    {
        assertThat(PartitionFilter.toLakehouseFilter(
                List.of(column("region", "string"), column("year", "int"), column("day", "date")),
                List.of("emea", "2026", "2026-01-31")))
                .isEqualTo("region='emea' and year=2026 and day=CAST('2026-01-31' as date)");
    }

    @Test
    void leavesOutAValueWithAQuoteFromAnExactFilter()
    {
        List<Column> columns = List.of(column("region", "string"), column("year", "int"));

        assertThat(PartitionFilter.toLakehouseFilter(columns, List.of("o'brien", "2026"))).isEqualTo("year=2026");
        assertThat(PartitionFilter.toLakehouseFilter(columns, List.of("o'brien", "it's"))).isEmpty();
    }

    @Test
    void refusesAMismatchBetweenTheColumnsAndTheValues()
    {
        assertThatThrownBy(() -> PartitionFilter.toLakehouseFilter(List.of(column("region", "string")), List.of("emea", "2026")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count");
    }

    @Test
    void reportsWhetherTheFilterLeftAValueOut()
    {
        assertThat(PartitionFilter.matchesExactly(List.of("emea", "2026"))).isTrue();
        assertThat(PartitionFilter.matchesExactly(List.of())).isTrue();
        assertThat(PartitionFilter.matchesExactly(List.of("emea", "o'brien"))).isFalse();
    }

    private static TupleDomain<String> singleValue(String column, Type type, Object value)
    {
        Object nativeValue = value instanceof String text ? utf8Slice(text) : value;
        return TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(type, nativeValue)));
    }

    private static Column column(String name, String hiveType)
    {
        return new Column(name, HiveType.valueOf(hiveType), Optional.empty(), Map.of());
    }
}
