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
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFakerPageSource
{
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
    public void testSplitsDoNotChangeValues()
    {
        List<FakerColumnHandle> columns = List.of(
                new FakerColumnHandle(0, "sequence", BIGINT, 0.5, null, Domain.all(BIGINT), ValueSet.of(BIGINT, 1L)),
                new FakerColumnHandle(1, "number", BIGINT, 0.5, null, Domain.all(BIGINT), ValueSet.none(BIGINT)),
                new FakerColumnHandle(2, "text", VARCHAR, 0.5, null, Domain.all(VARCHAR), ValueSet.none(VARCHAR)));
        List<List<Object>> singleSplit = generateRows(columns, 0, 5000);
        List<List<Object>> twoSplits = ImmutableList.<List<Object>>builder()
                .addAll(generateRows(columns, 0, 1234))
                .addAll(generateRows(columns, 1234, 5000 - 1234))
                .build();
        assertThat(twoSplits).isEqualTo(singleSplit);
    }

    private static FakerColumnHandle column(Domain domain)
    {
        return new FakerColumnHandle(0, "x", DOUBLE, 0, null, domain, ValueSet.none(DOUBLE));
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
