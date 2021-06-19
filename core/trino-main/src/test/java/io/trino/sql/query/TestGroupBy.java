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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGroupBy
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
    public void testCastDifferentCase()
    {
        // CAST type in a different case
        assertThat(assertions.query(
                "SELECT CAST(x AS bigint) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(x AS BIGINT)"))
                .matches("VALUES BIGINT '42'");

        // same expression including ROW with a delimited field name
        assertThat(assertions.query(
                "SELECT CAST(row(x) AS row(\"A\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" bigint))"))
                .matches("SELECT CAST(row(BIGINT '42') AS row(\"A\" bigint))");

        // ROW field name in a different case, not delimited
        assertThat(assertions.query(
                "SELECT CAST(row(x) AS row(abc bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(ABC bigint))"))
                .matches("SELECT CAST(row(BIGINT '42') AS row(abc bigint))");

        // ROW field type in a different case
        assertThat(assertions.query(
                "SELECT CAST(row(x) AS row(\"A\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" BigINT))"))
                .matches("SELECT CAST(row(BIGINT '42') AS row(\"A\" bigint))");

        // ROW field name in a different case, delimited
        assertThatThrownBy(() -> assertions.query(
                "SELECT CAST(row(x) AS row(\"a\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" bigint))"))
                .hasMessage("line 1:8: 'CAST(ROW (x) AS ROW(\"a\" bigint))' must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testDuplicateComplexExpressions()
    {
        assertThat(assertions.query(
                "SELECT a + 1, a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY 1, 2"))
                .matches("VALUES (2, 2)");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY a + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.a + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY A + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1, 1"))
                .matches("VALUES 2");
    }

    @Test
    public void testReferenceWithMixedStyle()
    {
        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY A + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.a + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT t.a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY a + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT t.a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY A + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT t.a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1"))
                .matches("VALUES 2");
    }

    @Test
    public void testGroupByRepeatedOrdinals()
    {
        assertThat(assertions.query(
                "SELECT null GROUP BY 1, 1"))
                .matches("VALUES null");
    }
}
