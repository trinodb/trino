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
package io.trino.parquet;

import io.trino.parquet.BenchmarkPageBoundsFilter.Query;
import io.trino.parquet.BenchmarkPageBoundsFilter.RowOrder;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBenchmarkPageBoundsFilter
{
    private static final double SCALE_FACTOR = 0.01;

    @Test
    public void testBenchmark()
            throws Throwable
    {
        for (RowOrder rowOrder : RowOrder.values()) {
            for (Query query : Query.values()) {
                for (int rowGroupRows : new int[] {1_000, 20_000}) {
                    for (boolean readerPushdown : new boolean[] {false, true}) {
                        for (boolean pageBounds : new boolean[] {false, true}) {
                            BenchmarkPageBoundsFilter benchmark = new BenchmarkPageBoundsFilter();
                            benchmark.scaleFactor = SCALE_FACTOR;
                            benchmark.rowOrder = rowOrder;
                            benchmark.query = query;
                            benchmark.rowGroupRows = rowGroupRows;
                            benchmark.readerPushdown = readerPushdown;
                            benchmark.pageBounds = pageBounds;
                            benchmark.setup();
                            assertThat(benchmark.scan())
                                    .describedAs("%s %s rowGroupRows=%s readerPushdown=%s pageBounds=%s", rowOrder, query, rowGroupRows, readerPushdown, pageBounds)
                                    .isEqualTo(expectedRows(query));
                        }
                    }
                }
            }
        }
    }

    private static long expectedRows(Query query)
    {
        int minShipDate = (int) LocalDate.parse("1994-01-01").toEpochDay();
        int maxShipDate = (int) LocalDate.parse("1995-01-01").toEpochDay();
        long rows = 0;
        for (LineItem lineItem : new LineItemGenerator(SCALE_FACTOR, 1, 1)) {
            boolean selected = lineItem.shipDate() >= minShipDate && lineItem.shipDate() < maxShipDate;
            if (query == Query.Q6) {
                selected = selected && lineItem.discount() >= 0.05 && lineItem.discount() <= 0.07 && lineItem.quantity() < 24;
            }
            if (selected) {
                rows++;
            }
        }
        return rows;
    }
}
