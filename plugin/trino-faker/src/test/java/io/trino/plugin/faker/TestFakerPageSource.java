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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.trino.plugin.faker.Distribution.UNIFORM;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFakerPageSource
{
    // an orderable type without a generator, like BingTile from the geospatial plugin
    private static final Type ORDERABLE_TYPE = new AbstractLongType(new TypeDescriptor("orderable"))
    {
        @Override
        public Object getObjectValue(Block block, int position)
        {
            return block.isNull(position) ? null : getLong(block, position);
        }
    };

    @Test
    public void testNaNGeneration()
    {
        FakerColumnHandle column = column(Domain.singleValue(DOUBLE, Double.NaN));
        FakerPageSource source = new FakerPageSource(Locale.ENGLISH, 0, List.of(column), 0, 1);
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(DOUBLE.getDouble(source.getNextSourcePage().getBlock(0), 0)).isNaN();
    }

    @Test
    public void testUnsupportedNaNRange()
    {
        for (ValueSet values : List.of(
                ValueSet.of(DOUBLE, Double.NaN).complement(),
                ValueSet.ofRanges(Range.lessThan(DOUBLE, 1.0)).union(ValueSet.of(DOUBLE, Double.NaN)))) {
            assertThatThrownBy(() -> new FakerPageSource(Locale.ENGLISH, 0, List.of(column(Domain.create(values, false))), 0, 1))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Non-discrete NaN constraints are not supported for generated columns");
        }
    }

    @Test
    public void testOrderableTypeWithoutGenerator()
    {
        FakerColumnHandle column = new FakerColumnHandle(0, "x", ORDERABLE_TYPE, 0, null, Domain.all(ORDERABLE_TYPE), ValueSet.none(ORDERABLE_TYPE), Optional.empty(), UNIFORM, List.of());
        FakerPageSource source = new FakerPageSource(Locale.ENGLISH, 0, List.of(column), 0, 1);
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.getNextSourcePage().getBlock(0).isNull(0)).isTrue();
    }

    @Test
    public void testInvalidGeneratedValue()
    {
        FakerColumnHandle column = new FakerColumnHandle(0, "x", INTEGER, 0, "#{Name.first_name}", Domain.all(INTEGER), ValueSet.none(INTEGER), Optional.empty(), UNIFORM, List.of());
        FakerPageSource source = new FakerPageSource(Locale.ENGLISH, 0, List.of(column), 0, 1);
        assertThatThrownBy(source::getNextSourcePage)
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("The `generator` property of column `x` produced `\\w+`, which is not a valid integer value");
    }

    @Test
    public void testSplitsDoNotChangeValues()
    {
        List<FakerColumnHandle> columns = List.of(
                new FakerColumnHandle(0, "sequence", BIGINT, 0.5, null, Domain.all(BIGINT), ValueSet.of(BIGINT, 1L), Optional.empty(), UNIFORM, List.of()),
                new FakerColumnHandle(1, "number", BIGINT, 0.5, null, Domain.all(BIGINT), ValueSet.none(BIGINT), Optional.empty(), UNIFORM, List.of()),
                new FakerColumnHandle(2, "text", VARCHAR, 0.5, null, Domain.all(VARCHAR), ValueSet.none(VARCHAR), Optional.empty(), UNIFORM, List.of()));
        List<List<Object>> singleSplit = generateRows(columns, 0, 5000);
        List<List<Object>> twoSplits = ImmutableList.<List<Object>>builder()
                .addAll(generateRows(columns, 0, 1234))
                .addAll(generateRows(columns, 1234, 5000 - 1234))
                .build();
        assertThat(twoSplits).isEqualTo(singleSplit);
    }

    @Test
    public void testDistantRowsDoNotRepeatValues()
    {
        List<FakerColumnHandle> columns = List.of(new FakerColumnHandle(0, "number", BIGINT, 0, null, Domain.all(BIGINT), ValueSet.none(BIGINT), Optional.empty(), UNIFORM, List.of()));
        // rows start at an offset of the column's random stream, which wraps around after 2^44 rows
        assertThat(generateRows(columns, 1L << 44, 1000)).doesNotContainAnyElementsOf(generateRows(columns, 0, 1000));
        assertThat(generateRows(columns, 3L << 44, 1000)).doesNotContainAnyElementsOf(generateRows(columns, 1L << 44, 1000));
    }

    @Test
    public void testDeeplyNestedPageSize()
    {
        Type type = INTEGER;
        for (int depth = 0; depth < 8; depth++) {
            type = new ArrayType(type);
        }
        FakerColumnHandle column = new FakerColumnHandle(0, "nested", type, 0, null, Domain.all(type), ValueSet.none(type), Optional.empty(), UNIFORM, List.of());
        FakerPageSource source = new FakerPageSource(Locale.ENGLISH, 0, List.of(column), 0, 100_000);
        SourcePage page = source.getNextSourcePage();
        while (page == null) {
            page = source.getNextSourcePage();
        }
        // every row has about 2500 values on average, so a page of 4096 rows would take tens of megabytes
        assertThat(page.getSizeInBytes()).isLessThan(8 * 1024 * 1024);
    }

    private static FakerColumnHandle column(Domain domain)
    {
        return new FakerColumnHandle(0, "x", DOUBLE, 0, null, domain, ValueSet.none(DOUBLE), Optional.empty(), UNIFORM, List.of());
    }

    private static List<List<Object>> generateRows(List<FakerColumnHandle> columns, long rowOffset, long limit)
    {
        FakerPageSource source = new FakerPageSource(Locale.ENGLISH, 0, columns, rowOffset, limit);
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page == null) {
                continue;
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                // a list that allows nulls
                List<Object> row = new ArrayList<>();
                for (int channel = 0; channel < columns.size(); channel++) {
                    row.add(columns.get(channel).type().getObjectValue(page.getBlock(channel), position));
                }
                rows.add(row);
            }
        }
        return rows.build();
    }
}
