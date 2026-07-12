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
}
