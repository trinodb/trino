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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBuiltInConstantSpecializations
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
    public void testDynamicStringArguments()
    {
        assertThat(assertions.query(
                """
                SELECT word_stem(word, language)
                FROM (VALUES ('authorized', 'en'), ('continuerait', 'fr')) t(word, language)
                """))
                .matches("VALUES CAST('author' AS varchar(12)), CAST('continu' AS varchar(12))");

        assertThat(assertions.query(
                """
                SELECT translate(value, source, target)
                FROM (VALUES ('abcda', 'a', 'z'), ('abcd', 'ac', 'z')) t(value, source, target)
                """))
                .matches("VALUES VARCHAR 'zbcdz', VARCHAR 'zbd'");

        assertThat(assertions.query(
                """
                SELECT from_utf8(value, replacement)
                FROM (VALUES (X'58BF', '#'), (X'58BF', '')) t(value, replacement)
                """))
                .matches("VALUES VARCHAR 'X#', VARCHAR 'X'");

        assertThat(assertions.query(
                """
                SELECT from_utf8(value, replacement)
                FROM (VALUES (X'58BF', BIGINT '35'), (X'58BF', BIGINT '36')) t(value, replacement)
                """))
                .matches("VALUES VARCHAR 'X#', VARCHAR 'X$'");
    }

    @Test
    public void testDynamicFormatArguments()
    {
        assertThat(assertions.query(
                """
                SELECT format_datetime(value, format)
                FROM (VALUES
                    (TIMESTAMP '2020-05-10 12:34:56', 'yyyy-MM-dd'),
                    (TIMESTAMP '2020-05-10 12:34:56', 'HH:mm:ss')) t(value, format)
                """))
                .matches("VALUES VARCHAR '2020-05-10', VARCHAR '12:34:56'");

        assertThat(assertions.query(
                """
                SELECT date_format(value, format)
                FROM (VALUES
                    (TIMESTAMP '2020-05-10 12:34:56', '%Y-%m-%d'),
                    (TIMESTAMP '2020-05-10 12:34:56', '%H:%i:%s')) t(value, format)
                """))
                .matches("VALUES VARCHAR '2020-05-10', VARCHAR '12:34:56'");

        assertThat(assertions.query(
                """
                SELECT date_parse(value, format)
                FROM (VALUES ('2020-05-10', '%Y-%m-%d'), ('12:34:56', '%H:%i:%s')) t(value, format)
                """))
                .matches("VALUES TIMESTAMP '2020-05-10 00:00:00.000', TIMESTAMP '1970-01-01 12:34:56.000'");

        assertThat(assertions.query(
                """
                SELECT parse_datetime(value, format)
                FROM (VALUES ('2020-05-10 12:34 +0000', 'yyyy-MM-dd HH:mm Z')) t(value, format)
                """))
                .matches("VALUES TIMESTAMP '2020-05-10 12:34:00.000 +00:00'");
    }

    @Test
    public void testDynamicDateUnits()
    {
        assertThat(assertions.query(
                """
                SELECT date_trunc(unit, value), date_add(unit, 2, value), date_diff(unit, value, DATE '2022-05-17')
                FROM (VALUES
                    ('day', DATE '2022-05-10'),
                    ('month', DATE '2022-03-10')) t(unit, value)
                """))
                .matches(
                        """
                        VALUES
                            (DATE '2022-05-10', DATE '2022-05-12', BIGINT '7'),
                            (DATE '2022-03-01', DATE '2022-05-10', BIGINT '2')
                        """);

        assertThat(assertions.query(
                """
                SELECT date_trunc(unit, value), date_add(unit, 2, value), date_diff(unit, value, TIMESTAMP '2022-05-17 12:00:00')
                FROM (VALUES
                    ('day', TIMESTAMP '2022-05-10 12:34:56'),
                    ('month', TIMESTAMP '2022-03-10 12:34:56')) t(unit, value)
                """))
                .matches(
                        """
                        VALUES
                            (TIMESTAMP '2022-05-10 00:00:00', TIMESTAMP '2022-05-12 12:34:56', BIGINT '6'),
                            (TIMESTAMP '2022-03-01 00:00:00', TIMESTAMP '2022-05-10 12:34:56', BIGINT '2')
                        """);

        assertThat(assertions.query(
                """
                SELECT date_trunc(unit, value), date_add(unit, 2, value), date_diff(unit, value, TIMESTAMP '2022-05-17 12:00:00 UTC')
                FROM (VALUES
                    ('day', TIMESTAMP '2022-05-10 12:34:56 UTC'),
                    ('month', TIMESTAMP '2022-03-10 12:34:56 UTC')) t(unit, value)
                """))
                .matches(
                        """
                        VALUES
                            (TIMESTAMP '2022-05-10 00:00:00 UTC', TIMESTAMP '2022-05-12 12:34:56 UTC', BIGINT '6'),
                            (TIMESTAMP '2022-03-01 00:00:00 UTC', TIMESTAMP '2022-05-10 12:34:56 UTC', BIGINT '2')
                        """);
    }
}
