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

import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayExceptFunction
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
    public void testBasic()
    {
        assertThat(assertions.function("array_except", "ARRAY[1, 5, 3]", "ARRAY[3]"))
                .matches("ARRAY[1, 5]");

        assertThat(assertions.function("array_except", "ARRAY[BIGINT '1', 5, 3]", "ARRAY[5]"))
                .matches("ARRAY[BIGINT '1', BIGINT '3']");

        assertThat(assertions.function("array_except", "ARRAY[VARCHAR 'x', 'y', 'z']", "ARRAY['x']"))
                .matches("ARRAY[VARCHAR 'y', VARCHAR 'z']");

        assertThat(assertions.function("array_except", "ARRAY[true, false, null]", "ARRAY[true]"))
                .matches("ARRAY[false, null]");

        assertThat(assertions.function("array_except", "ARRAY[1.1E0, 5.4E0, 3.9E0]", "ARRAY[5, 5.4E0]"))
                .matches("ARRAY[1.1E0, 3.9E0]");
    }

    @Test
    public void testEmpty()
    {
        assertThat(assertions.function("array_except", "ARRAY[]", "ARRAY[]"))
                .matches("ARRAY[]");

        assertThat(assertions.function("array_except", "ARRAY[]", "ARRAY[1, 3]"))
                .matches("CAST(ARRAY[] AS array(integer))");

        assertThat(assertions.function("array_except", "ARRAY[VARCHAR 'abc']", "ARRAY[]"))
                .matches("ARRAY[VARCHAR 'abc']");
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.function("array_except", "ARRAY[NULL]", "NULL"))
                .isNull(new ArrayType(UNKNOWN));

        assertThat(assertions.function("array_except", "NULL", "NULL"))
                .isNull(new ArrayType(UNKNOWN));

        assertThat(assertions.function("array_except", "NULL", "ARRAY[NULL]"))
                .isNull(new ArrayType(UNKNOWN));

        assertThat(assertions.function("array_except", "ARRAY[NULL]", "ARRAY[NULL]"))
                .matches("ARRAY[]");

        assertThat(assertions.function("array_except", "ARRAY[]", "ARRAY[NULL]"))
                .matches("ARRAY[]");

        assertThat(assertions.function("array_except", "ARRAY[NULL]", "ARRAY[]"))
                .matches("ARRAY[NULL]");
    }

    @Test
    public void testDuplicates()
    {
        assertThat(assertions.function("array_except", "ARRAY[1, 5, 3, 5, 1]", "ARRAY[3]"))
                .matches("ARRAY[1, 5]");

        assertThat(assertions.function("array_except", "ARRAY[BIGINT '1', 5, 5, 3, 3, 3, 1]", "ARRAY[3, 5]"))
                .matches("ARRAY[BIGINT '1']");

        assertThat(assertions.function("array_except", "ARRAY[VARCHAR 'x', 'x', 'y', 'z']", "ARRAY['x', 'y', 'x']"))
                .matches("ARRAY[VARCHAR 'z']");

        assertThat(assertions.function("array_except", "ARRAY[true, false, null, true, false, null]", "ARRAY[true, true, true]"))
                .matches("ARRAY[false, null]");
    }

    @Test
    public void testNonDistinctNonEqualValues()
    {
        assertThat(assertions.function("array_except", "ARRAY[NaN()]", "ARRAY[NaN()]"))
                .matches("CAST(ARRAY[] AS array(double))");

        assertThat(assertions.function("array_except", "ARRAY[1, NaN(), 3]", "ARRAY[NaN(), 3]"))
                .matches("ARRAY[1E0]");
    }
}
