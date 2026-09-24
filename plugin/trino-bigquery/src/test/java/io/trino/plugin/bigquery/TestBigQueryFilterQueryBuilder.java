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
package io.trino.plugin.bigquery;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.google.cloud.bigquery.Field.Mode.NULLABLE;
import static com.google.cloud.bigquery.StandardSQLTypeName.FLOAT64;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryFilterQueryBuilder
{
    private static final BigQueryColumnHandle COLUMN = new BigQueryColumnHandle("x", List.of(), DOUBLE, FLOAT64, true, NULLABLE, List.of(), null, false);

    @Test
    public void testNaNMembership()
    {
        assertFilter(Domain.singleValue(DOUBLE, Double.NaN), "(IS_NAN(`x`))");
        assertFilter(Domain.create(ValueSet.of(DOUBLE, Double.NaN).complement(), false), "(NOT IS_NAN(`x`))");
        assertFilter(Domain.create(ValueSet.of(DOUBLE, Double.NaN), true), "(IS_NAN(`x`) OR `x` IS NULL)");
        assertFilter(Domain.create(ValueSet.of(DOUBLE, Double.NaN).complement(), true), "(NOT IS_NAN(`x`) OR `x` IS NULL)");
        assertFilter(Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 1.0)).union(ValueSet.of(DOUBLE, Double.NaN)), false),
                "((`x` < CAST('1.0' AS float64)) OR IS_NAN(`x`))");
        assertFilter(Domain.notNull(DOUBLE), "`x` IS NOT NULL");
    }

    private static void assertFilter(Domain domain, String expected)
    {
        assertThat(BigQueryFilterQueryBuilder.buildFilter(TupleDomain.withColumnDomains(Map.of(COLUMN, domain)))).contains(expected);
    }
}
