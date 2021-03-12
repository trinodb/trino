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
package io.trino.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestValues
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testRowSpecifications()
    {
        assertThat(assertions.query("VALUES 1, 2"))
                .matches("VALUES ROW(1), ROW(2)");

        assertThat(assertions.query("VALUES (1, 'a')"))
                .matches("VALUES ROW(1, 'a')");

        assertThat(assertions.query("VALUES CAST(ROW(1, 'TruE') AS row(double, boolean))"))
                .matches("VALUES ROW(1e0, true)");

        // mixed specifications
        assertThat(assertions.query("VALUES " +
                "CAST(ROW(1, 'TruE') AS row(double, boolean)), " +
                "ROW(2e0, false), " +
                "(3e0, null)"))
                .matches("VALUES " +
                        "ROW(1e0, true)," +
                        "ROW(2e0, false), " +
                        "ROW(3e0, null)");

        assertThat(assertions.query("SELECT key FROM " +
                "(VALUES " +
                "CAST(ROW(1, 'TruE') AS row(double, boolean)), " +
                "ROW(2e0, false), " +
                "(3e0, null)) T(key, value)"))
                .matches("VALUES 1e0, 2e0, 3e0");
    }

    @Test
    public void testCoercions()
    {
        assertThat(assertions.query("VALUES 1, 2e0"))
                .matches("VALUES ROW(1e0), ROW(2e0)");

        assertThat(assertions.query("VALUES (1, 2), (3, 4e0)"))
                .matches("VALUES ROW(1, 2e0), ROW(3, 4e0)");

        assertThat(assertions.query("VALUES CAST(ROW(1, 2) AS row(integer, smallint)), (3, 4e0)"))
                .matches("VALUES ROW(1, 2e0), ROW(3, 4e0)");

        assertThat(assertions.query("VALUES CAST(ROW(1, 2) AS row(integer, double)), (3, 4)"))
                .matches("VALUES ROW(1, 2e0), ROW(3, 4e0)");

        assertThat(assertions.query("VALUES CAST(ROW(1, null) AS row(integer, double)), (3, 4)"))
                .matches("VALUES ROW(1, null), ROW(3, 4e0)");
    }

    @Test
    public void testNulls()
    {
        assertThat(assertions.query("VALUES null"))
                .matches("VALUES ROW(null)");

        assertThat(assertions.query("VALUES (null, null)"))
                .matches("VALUES ROW(null, null)");

        assertThat(assertions.query("VALUES (null, null), ('a', 'b')"))
                .matches("VALUES ROW(null, null), ROW('a', 'b')");

        assertThat(assertions.query("VALUES " +
                "CAST(ROW(null, null) AS row(real, double)), " +
                "(1, 1)"))
                .matches("VALUES " +
                        "ROW(null, null), " +
                        "ROW(REAL '1', DOUBLE '1')");
    }

    @Test
    public void testFailingExpression()
    {
        assertTrinoExceptionThrownBy(() -> assertions.query("VALUES 0 / 0"))
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(() -> assertions.query("VALUES CASE 1 WHEN 0 THEN true WHEN 0 / 0 THEN false END"))
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(() -> assertions.query("VALUES IF(0 / 0 > 0, true, false)"))
                .hasErrorCode(DIVISION_BY_ZERO);
    }
}
