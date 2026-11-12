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
package io.trino.type;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.COLLATION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCollatedVarcharQueries
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testCast()
    {
        // casting to a collated type keeps the value and changes the type
        assertThat(assertions.expression("CAST(a AS varchar(10) COLLATE unicode_ci)")
                .binding("a", "'Müller'"))
                .hasType(createVarcharType(10, collationRegistry().getCollation("unicode_ci")))
                .isEqualTo("Müller");

        // casts within a collation, and back to the default collation, keep the value
        assertThat(assertions.expression("CAST(CAST(a AS varchar(3) COLLATE unicode) AS varchar(10) COLLATE unicode)")
                .binding("a", "'abc'"))
                .hasType(createVarcharType(10, collationRegistry().getCollation("unicode")))
                .isEqualTo("abc");
        assertThat(assertions.expression("CAST(CAST(a AS varchar COLLATE unicode) AS varchar)")
                .binding("a", "'abc'"))
                .isEqualTo("abc");

        // a narrowing cast truncates as for the default collation
        assertThat(assertions.expression("CAST(a AS varchar(3) COLLATE unicode)")
                .binding("a", "'banana'"))
                .isEqualTo("ban");

        assertThat(assertions.query("SELECT CAST('a' AS varchar COLLATE no_such_collation)"))
                .failure().hasErrorCode(COLLATION_NOT_FOUND);
        // a collation name is an identifier: unquoted it folds to lower case, quoted it is taken as written
        for (String collation : new String[] {"UNICODE_CI", "\"unicode_ci\""}) {
            assertThat(assertions.expression("CAST(a AS varchar(10) COLLATE %s)".formatted(collation))
                    .binding("a", "'x'"))
                    .hasType(createVarcharType(10, collationRegistry().getCollation("unicode_ci")));
        }
        assertThat(assertions.query("SELECT CAST('a' AS varchar COLLATE \"UNICODE_CI\")"))
                .failure().hasErrorCode(COLLATION_NOT_FOUND);
        // only character strings have a collation
        assertThat(assertions.query("SELECT CAST(1 AS bigint COLLATE unicode)"))
                .failure().hasErrorCode(TYPE_MISMATCH).hasMessage("COLLATE is not supported for type bigint");
        assertThat(assertions.query("SELECT CAST(ARRAY[1] AS array(bigint COLLATE unicode))"))
                .failure().hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testComparisonWithinCollation()
    {
        // operands of one collation compare under it
        assertThat(assertions.query(
                "SELECT CAST('MÜLLER' AS varchar COLLATE unicode_ci) = CAST('müller' AS varchar COLLATE unicode_ci)"))
                .matches("VALUES true");
        assertThat(assertions.query(
                "SELECT CAST('Müller' AS varchar COLLATE unicode) < CAST('Mutter' AS varchar COLLATE unicode)"))
                .matches("VALUES true");
        // code point order puts ü after t
        assertThat(assertions.query("SELECT 'Müller' < 'Mutter'"))
                .matches("VALUES false");
    }

    @Test
    public void testGroupingAndOrdering()
    {
        assertThat(assertions.query(
                """
                SELECT count(*) FROM (
                    SELECT DISTINCT CAST(name AS varchar COLLATE unicode_ci)
                    FROM (VALUES 'Alice', 'ALICE', 'alice', 'Bob') t(name))
                """))
                .matches("VALUES BIGINT '2'");
        assertThat(assertions.query(
                """
                SELECT CAST(name AS varchar)
                FROM (VALUES 'z', 'ä', 'a') t(name)
                ORDER BY name
                """))
                .ordered()
                .matches("VALUES varchar 'a', varchar 'z', varchar 'ä'");
        assertThat(assertions.query(
                """
                SELECT CAST(name AS varchar)
                FROM (VALUES 'z', 'ä', 'a') t(name)
                ORDER BY CAST(name AS varchar COLLATE unicode)
                """))
                .ordered()
                .matches("VALUES varchar 'a', varchar 'ä', varchar 'z'");
    }

    @Test
    public void testEqualValuesStayDistinct()
    {
        // equal under the collation is not the same string: projections must keep each side's value
        assertThat(assertions.query(
                """
                SELECT CAST(l AS varchar), CAST(r AS varchar)
                FROM (SELECT CAST(x AS varchar COLLATE unicode_ci) l FROM (VALUES 'Alice') t(x))
                JOIN (SELECT CAST(y AS varchar COLLATE unicode_ci) r FROM (VALUES 'ALICE') t(y))
                ON l = r
                """))
                .matches("VALUES (varchar 'Alice', varchar 'ALICE')");
        // a predicate on one join key must not be transferred to the other through the equality
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT CAST(x AS varchar COLLATE unicode_ci) l FROM (VALUES 'Alice') t(x))
                JOIN (SELECT CAST(y AS varchar COLLATE unicode_ci) r FROM (VALUES 'ALICE') t(y))
                ON l = r
                WHERE CAST(l AS varchar) = 'Alice'
                """))
                .matches("VALUES BIGINT '1'");
        // a filter equal to a constant must not replace the column by the constant
        assertThat(assertions.query(
                """
                SELECT CAST(l AS varchar)
                FROM (SELECT CAST(x AS varchar COLLATE unicode_ci) l FROM (VALUES 'Alice', 'ALICE') t(x))
                WHERE l = CAST('alice' AS varchar COLLATE unicode_ci)
                """))
                .matches("VALUES varchar 'Alice', varchar 'ALICE'");
    }

    @Test
    public void testJoinOverConstantRows()
    {
        // the planner derives a predicate from the rows of a VALUES; for a collated column the
        // rows are not a domain, and the constants here are not in byte order
        for (String collation : new String[] {"unicode", "unicode_ci"}) {
            assertThat(assertions.query(
                    """
                    SELECT count(*)
                    FROM (VALUES CAST('zitrone' AS varchar COLLATE %1$s), CAST('Äpfel' AS varchar COLLATE %1$s), CAST('Birne' AS varchar COLLATE %1$s), CAST(NULL AS varchar COLLATE %1$s)) a(k)
                    JOIN (VALUES CAST('Äpfel' AS varchar COLLATE %1$s), CAST('birne' AS varchar COLLATE %1$s)) b(k)
                    ON a.k = b.k
                    """.formatted(collation)))
                    .as(collation)
                    .matches(collation.equals("unicode") ? "VALUES BIGINT '1'" : "VALUES BIGINT '2'");
        }
    }

    @Test
    public void testConstants()
    {
        // equal constants of a collated type are distinct values: neither list deduplication,
        // subscripts, nor a comparison with a constant may replace one value with another
        assertThat(assertions.query(
                "SELECT CAST(x AS varchar) FROM (VALUES CAST('Alice' AS varchar COLLATE unicode_ci), CAST('ALICE' AS varchar COLLATE unicode_ci)) t(x) WHERE x IN (CAST('alice' AS varchar COLLATE unicode_ci), CAST('ALICE' AS varchar COLLATE unicode_ci))"))
                .matches("VALUES varchar 'Alice', varchar 'ALICE'");
        assertThat(assertions.query(
                "SELECT ARRAY[CAST('Alice' AS varchar COLLATE unicode_ci), CAST('ALICE' AS varchar COLLATE unicode_ci)][2]"))
                .matches("VALUES CAST('ALICE' AS varchar COLLATE unicode_ci)");
        assertThat(assertions.query(
                "SELECT CAST(x AS varchar) FROM (VALUES CAST('Alice' AS varchar COLLATE unicode_ci)) t(x) WHERE x = CAST('ALICE' AS varchar COLLATE unicode_ci)"))
                .matches("VALUES varchar 'Alice'");
    }

    @Test
    public void testNoImplicitCollationChange()
    {
        // different collations never combine implicitly
        assertThat(assertions.query(
                "SELECT CAST('a' AS varchar COLLATE unicode) = CAST('a' AS varchar COLLATE unicode_ci)"))
                .failure().hasMessageContaining("Cannot apply operator");
        assertThat(assertions.query(
                "SELECT CAST('a' AS varchar COLLATE unicode) = 'a'"))
                .failure().hasMessageContaining("Cannot apply operator");
        // pattern matching uses code points, so it does not accept a collated value
        assertThat(assertions.query("SELECT CAST('ALICE' AS varchar COLLATE unicode_ci) LIKE 'al%'"))
                .failure().hasMessageContaining("Unexpected parameters (varchar COLLATE \"unicode_ci\", LikePattern) for function $like");
    }
}
