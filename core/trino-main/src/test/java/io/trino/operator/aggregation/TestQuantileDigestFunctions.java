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
package io.trino.operator.aggregation;

import io.airlift.stats.QuantileDigest;
import io.trino.spi.type.SqlVarbinary;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestQuantileDigestFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testQuantileAtValueBigint()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        addAll(qdigest, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(bigint)), 20)")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isNull();
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(bigint)), 6)")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isEqualTo(0.6);
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(bigint)), -1)")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isNull();
    }

    @Test
    public void testQuantileAtValueDouble()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        IntStream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .mapToLong(FloatingPointBitsConverterUtil::doubleToSortableLong)
                .forEach(qdigest::add);
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(double)), 5.6)")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isEqualTo(0.6);
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(double)), -1.23)")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isNull();
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(double)), 12.3)")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isNull();
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(double)), nan())")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isNull();
    }

    @Test
    public void testQuantileAtValueBigintWithEmptyDigest()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(bigint)), 5)")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isNull();
    }

    @Test
    public void testQuantileRoundTrip()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        addAll(qdigest, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(assertions
                .expression("value_at_quantile(CAST(a AS qdigest(bigint)), quantile_at_value(CAST(a AS qdigest(bigint)), 6))")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isEqualTo(6L);
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(bigint)), value_at_quantile(CAST(a AS qdigest(bigint)), .6))")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isEqualTo(.6);

        qdigest = new QuantileDigest(1);
        IntStream.range(0, 10)
                .mapToLong(FloatingPointBitsConverterUtil::doubleToSortableLong)
                .forEach(qdigest::add);

        assertThat(assertions
                .expression("value_at_quantile(CAST(a AS qdigest(double)),quantile_at_value(CAST(a AS qdigest(double)), 5.6))")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isEqualTo(6.);
        assertThat(assertions
                .expression("quantile_at_value(CAST(a AS qdigest(double)),value_at_quantile(CAST(a AS qdigest(double)), .6))")
                .binding("a", "X'%s'".formatted(toHexString(qdigest))))
                .isEqualTo(.6);
    }

    private static void addAll(QuantileDigest digest, long... values)
    {
        for (long value : values) {
            digest.add(value);
        }
    }

    private static String toHexString(QuantileDigest qdigest)
    {
        return new SqlVarbinary(qdigest.serialize().getBytes()).toString().replaceAll("\\s+", " ");
    }
}
