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
package io.trino.operator.scalar;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestArrayVectorFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testEuclideanDistance()
    {
        assertThat(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[2]"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("euclidean_distance", "ARRAY[1, 2]", "ARRAY[3, 4]"))
                .hasType(DOUBLE)
                .isEqualTo(2.8284271247461903);
        assertThat(assertions.function("euclidean_distance", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[4.4, 5.5, 6.6]"))
                .hasType(DOUBLE)
                .isEqualTo(5.715767651212193);
        assertThat(assertions.function("euclidean_distance", "ARRAY[]", "ARRAY[]"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);

        // min and max
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(6.805646932770577E38);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(6.805646932770577E38);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(2.802596928649634E-45);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(2.802596928649634E-45);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(3.4028234663852886E38);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(3.4028234663852886E38);

        // NaN and infinity
        assertThat(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[CAST(nan() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("euclidean_distance", "ARRAY[CAST(nan() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[CAST(-infinity() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("euclidean_distance", "ARRAY[CAST(-infinity() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[CAST(+infinity() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("euclidean_distance", "ARRAY[CAST(+infinity() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);

        assertTrinoExceptionThrownBy(assertions.function("euclidean_distance", "ARRAY[]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[1, 2]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("euclidean_distance", "ARRAY[1, 2]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
    }

    @Test
    void testInnerProduct()
    {
        assertThat(assertions.function("inner_product", "ARRAY[1]", "ARRAY[2]"))
                .hasType(DOUBLE)
                .isEqualTo(-2.0);
        assertThat(assertions.function("inner_product", "ARRAY[1, 2]", "ARRAY[3, 4]"))
                .hasType(DOUBLE)
                .isEqualTo(-11.0);
        assertThat(assertions.function("inner_product", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[4.4, 5.5, 6.6]"))
                .hasType(DOUBLE)
                .isEqualTo(-38.719999842643745);
        assertThat(assertions.function("inner_product", "ARRAY[]", "ARRAY[]"))
                .hasType(DOUBLE)
                .isEqualTo(-0.0);

        // min and max
        assertThat(assertions.function("inner_product", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.1579207543382391E77);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.1579207543382391E77);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.1579207543382391E77);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.1579207543382391E77);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.9636373861190906E-90);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.9636373861190906E-90);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.9636373861190906E-90);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.9636373861190906E-90);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(-4.7683712978141557E-7);
        assertThat(assertions.function("inner_product", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(-4.7683712978141557E-7);

        // NaN and infinity
        assertThat(assertions.function("inner_product", "ARRAY[1]", "ARRAY[CAST(nan() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("inner_product", "ARRAY[CAST(nan() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("inner_product", "ARRAY[1]", "ARRAY[CAST(-infinity() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("inner_product", "ARRAY[CAST(-infinity() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("inner_product", "ARRAY[1]", "ARRAY[CAST(+infinity() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(NEGATIVE_INFINITY);
        assertThat(assertions.function("inner_product", "ARRAY[CAST(+infinity() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NEGATIVE_INFINITY);

        assertTrinoExceptionThrownBy(assertions.function("inner_product", "ARRAY[]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("inner_product", "ARRAY[1]", "ARRAY[]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("inner_product", "ARRAY[1]", "ARRAY[1, 2]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("inner_product", "ARRAY[1, 2]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
    }

    @Test
    void testCosineDistance()
    {
        assertThat(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[2]"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[1, 2]", "ARRAY[3, 4]"))
                .hasType(DOUBLE)
                .isEqualTo(0.01613008990009257);
        assertThat(assertions.function("cosine_distance", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[4.4, 5.5, 6.6]"))
                .hasType(DOUBLE)
                .isEqualTo(0.025368154060122383);

        // min and max
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(2.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(2.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(2.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(2.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);

        // NaN and infinity
        assertThat(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[CAST(nan() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[CAST(nan() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[CAST(-infinity() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[CAST(-infinity() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[CAST(+infinity() AS real)]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[CAST(+infinity() AS real)]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);

        assertTrinoExceptionThrownBy(assertions.function("cosine_distance", "ARRAY[]", "ARRAY[]")::evaluate)
                .hasMessage("Vector magnitude cannot be zero");
        assertTrinoExceptionThrownBy(assertions.function("cosine_distance", "ARRAY[]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[1, 2]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("cosine_distance", "ARRAY[1, 2]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
    }
}
