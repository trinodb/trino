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
    private final QueryAssertions assertions = new QueryAssertions();

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
        assertThat(assertions.function("euclidean_distance", "ARRAY[4, 5, 6]", "ARRAY[4, 5, 6]"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("euclidean_distance", "ARRAY[REAL '1.1', REAL '2.2', REAL '3.3']", "ARRAY[REAL '4.4', REAL '5.5', REAL '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(5.715767651212193);
        assertThat(assertions.function("euclidean_distance", "ARRAY[DOUBLE '1.1', DOUBLE '2.2', DOUBLE '3.3']", "ARRAY[DOUBLE '4.4', DOUBLE '5.5', DOUBLE '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(5.715767664977295);
        assertThat(assertions.function("euclidean_distance", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[4.4, 5.5, 6.6]"))
                .hasType(DOUBLE)
                .isEqualTo(5.715767664977295);
        assertThat(assertions.function("euclidean_distance", "ARRAY[]", "ARRAY[]"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);

        // real type's min and max
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

        // double type's min and max
        assertThat(assertions.function("euclidean_distance", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("euclidean_distance", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("euclidean_distance", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("euclidean_distance", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);

        // NaN and infinity
        assertThat(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[nan()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("euclidean_distance", "ARRAY[nan()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[-infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("euclidean_distance", "ARRAY[-infinity()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("euclidean_distance", "ARRAY[1]", "ARRAY[infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("euclidean_distance", "ARRAY[infinity()]", "ARRAY[1]"))
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
    void testDotProduct()
    {
        assertThat(assertions.function("dot_product", "ARRAY[1]", "ARRAY[2]"))
                .hasType(DOUBLE)
                .isEqualTo(2.0);
        assertThat(assertions.function("dot_product", "ARRAY[1, 2]", "ARRAY[3, 4]"))
                .hasType(DOUBLE)
                .isEqualTo(11.0);
        assertThat(assertions.function("dot_product", "ARRAY[4, 5, 6]", "ARRAY[4, 5, 6]"))
                .hasType(DOUBLE)
                .isEqualTo(77.0);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '1.1', REAL '2.2', REAL '3.3']", "ARRAY[REAL '4.4', REAL '5.5', REAL '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(38.719999842643745);
        assertThat(assertions.function("dot_product", "ARRAY[DOUBLE '1.1', DOUBLE '2.2', DOUBLE '3.3']", "ARRAY[DOUBLE '4.4', DOUBLE '5.5', DOUBLE '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(38.72);
        assertThat(assertions.function("dot_product", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[4.4, 5.5, 6.6]"))
                .hasType(DOUBLE)
                .isEqualTo(38.72);
        assertThat(assertions.function("dot_product", "ARRAY[]", "ARRAY[]"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);

        // real type's min and max
        assertThat(assertions.function("dot_product", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.1579207543382391E77);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.1579207543382391E77);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.1579207543382391E77);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.1579207543382391E77);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.9636373861190906E-90);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.9636373861190906E-90);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.9636373861190906E-90);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(-1.9636373861190906E-90);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(4.7683712978141557E-7);
        assertThat(assertions.function("dot_product", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(4.7683712978141557E-7);

        // double type's min and max
        assertThat(assertions.function("dot_product", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("dot_product", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("dot_product", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(NEGATIVE_INFINITY);
        assertThat(assertions.function("dot_product", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(NEGATIVE_INFINITY);

        // NaN and infinity
        assertThat(assertions.function("dot_product", "ARRAY[1]", "ARRAY[nan()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("dot_product", "ARRAY[nan()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("dot_product", "ARRAY[1]", "ARRAY[-infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(NEGATIVE_INFINITY);
        assertThat(assertions.function("dot_product", "ARRAY[-infinity()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NEGATIVE_INFINITY);
        assertThat(assertions.function("dot_product", "ARRAY[1]", "ARRAY[+infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);
        assertThat(assertions.function("dot_product", "ARRAY[infinity()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(POSITIVE_INFINITY);

        assertTrinoExceptionThrownBy(assertions.function("dot_product", "ARRAY[]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("dot_product", "ARRAY[1]", "ARRAY[]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("dot_product", "ARRAY[1]", "ARRAY[1, 2]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("dot_product", "ARRAY[1, 2]", "ARRAY[1]")::evaluate)
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
        assertThat(assertions.function("cosine_distance", "ARRAY[4, 5, 6]", "ARRAY[4, 5, 6]"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);
        assertThat(assertions.function("cosine_distance", "ARRAY[REAL '1.1', REAL '2.2', REAL '3.3']", "ARRAY[REAL '4.4', REAL '5.5', REAL '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(0.025368154060122383);
        assertThat(assertions.function("cosine_distance", "ARRAY[DOUBLE '1.1', DOUBLE '2.2', DOUBLE '3.3']", "ARRAY[DOUBLE '4.4', DOUBLE '5.5', DOUBLE '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(0.025368153802923676);
        assertThat(assertions.function("cosine_distance", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[4.4, 5.5, 6.6]"))
                .hasType(DOUBLE)
                .isEqualTo(0.025368153802923676);

        // real type's min and max
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

        // double type's min and max
        assertThat(assertions.function("cosine_distance", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);

        // NaN and infinity
        assertThat(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[nan()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[nan()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[-infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[-infinity()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[1]", "ARRAY[infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_distance", "ARRAY[infinity()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);

        assertThat(assertions.function("cosine_distance", "ARRAY[1, 2]", "ARRAY[3, null]"))
                .isNull(DOUBLE);
        assertThat(assertions.function("cosine_distance", "ARRAY[1, null]", "ARRAY[3, 4]"))
                .isNull(DOUBLE);

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

    @Test
    void testCosineSimilarity()
    {
        assertThat(assertions.function("cosine_similarity", "ARRAY[1]", "ARRAY[2]"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[1, 2]", "ARRAY[3, 4]"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 0.01613008990009257);
        assertThat(assertions.function("cosine_similarity", "ARRAY[4, 5, 6]", "ARRAY[4, 5, 6]"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '1.1', REAL '2.2', REAL '3.3']", "ARRAY[REAL '4.4', REAL '5.5', REAL '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 0.025368154060122383);
        assertThat(assertions.function("cosine_similarity", "ARRAY[DOUBLE '1.1', DOUBLE '2.2', DOUBLE '3.3']", "ARRAY[DOUBLE '4.4', DOUBLE '5.5', DOUBLE '6.6']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 0.025368153802923676);
        assertThat(assertions.function("cosine_similarity", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[4.4, 5.5, 6.6]"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 0.025368153802923676);

        // real type's min and max
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '-3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 2.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '-3.4028235e+38f']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 2.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '-1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 2.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '-1.4E-45']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0 - 2.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '3.4028235e+38f']", "ARRAY[REAL '1.4E-45']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);
        assertThat(assertions.function("cosine_similarity", "ARRAY[REAL '1.4E-45']", "ARRAY[REAL '3.4028235e+38f']"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);

        // double type's min and max
        assertThat(assertions.function("cosine_similarity", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[DOUBLE '1.7976931348623157E+309']", "ARRAY[DOUBLE '-1.7976931348623157E+308']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[DOUBLE '-1.7976931348623157E+308']", "ARRAY[DOUBLE '1.7976931348623157E+309']"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);

        // NaN and infinity
        assertThat(assertions.function("cosine_similarity", "ARRAY[1]", "ARRAY[nan()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[nan()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[1]", "ARRAY[-infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[-infinity()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[1]", "ARRAY[infinity()]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);
        assertThat(assertions.function("cosine_similarity", "ARRAY[infinity()]", "ARRAY[1]"))
                .hasType(DOUBLE)
                .isEqualTo(NaN);

        assertThat(assertions.function("cosine_similarity", "ARRAY[1, 2]", "ARRAY[3, null]"))
                .isNull(DOUBLE);
        assertThat(assertions.function("cosine_similarity", "ARRAY[1, null]", "ARRAY[3, 4]"))
                .isNull(DOUBLE);

        assertTrinoExceptionThrownBy(assertions.function("cosine_similarity", "ARRAY[]", "ARRAY[]")::evaluate)
                .hasMessage("Vector magnitude cannot be zero");
        assertTrinoExceptionThrownBy(assertions.function("cosine_similarity", "ARRAY[]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("cosine_similarity", "ARRAY[1]", "ARRAY[]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("cosine_similarity", "ARRAY[1]", "ARRAY[1, 2]")::evaluate)
                .hasMessage("The arguments must have the same length");
        assertTrinoExceptionThrownBy(assertions.function("cosine_similarity", "ARRAY[1, 2]", "ARRAY[1]")::evaluate)
                .hasMessage("The arguments must have the same length");
    }
}
