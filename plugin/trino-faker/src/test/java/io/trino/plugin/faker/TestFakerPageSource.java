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

import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import net.datafaker.Faker;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFakerPageSource
{
    @Test
    public void testNaNGeneration()
    {
        Random random = new Random(0);
        FakerColumnHandle column = column(Domain.singleValue(DOUBLE, Double.NaN));
        FakerPageSource source = new FakerPageSource(new Faker(random), random, List.of(column), 0, 1);
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(DOUBLE.getDouble(source.getNextSourcePage().getBlock(0), 0)).isNaN();
    }

    @Test
    public void testUnsupportedNaNRange()
    {
        for (ValueSet values : List.of(
                ValueSet.of(DOUBLE, Double.NaN).complement(),
                ValueSet.ofRanges(Range.lessThan(DOUBLE, 1.0)).union(ValueSet.of(DOUBLE, Double.NaN)))) {
            Random random = new Random(0);
            assertThatThrownBy(() -> new FakerPageSource(new Faker(random), random, List.of(column(Domain.create(values, false))), 0, 1))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Non-discrete NaN constraints are not supported for generated columns");
        }
    }

    private static FakerColumnHandle column(Domain domain)
    {
        return new FakerColumnHandle(0, "x", DOUBLE, 0, null, domain, ValueSet.none(DOUBLE));
    }
}
