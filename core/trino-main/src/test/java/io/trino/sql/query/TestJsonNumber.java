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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.spi.StandardErrorCode.PATH_EVALUATION_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestJsonNumber
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testNumberEntryPoints()
    {
        assertThat(assertions.execute(
                """
                SELECT JSON_ARRAY(CAST(123 AS NUMBER) RETURNING JSON),
                       JSON_OBJECT('x': CAST(123 AS NUMBER) RETURNING JSON),
                       JSON_QUERY('{}', 'lax $p' PASSING CAST(123 AS NUMBER) AS "p" RETURNING JSON)
                """).getMaterializedRows().getFirst().getFields())
                .containsExactly("[123]", "{\"x\":123}", "123");
        assertThat(assertions.query("SELECT JSON_VALUE(JSON '123', 'lax $' RETURNING NUMBER)"))
                .matches("VALUES CAST(123 AS NUMBER)");
    }

    @Test
    void testNumberArraySubscripts()
    {
        for (String index : List.of("CAST(1 AS NUMBER)", "NUMBER '0.5'", "NUMBER '1.4'")) {
            assertThat(assertions.query("SELECT JSON_VALUE(JSON '[10,20,30]', 'strict $[$i]' PASSING " + index + " AS \"i\" RETURNING BIGINT ERROR ON ERROR)"))
                    .matches("VALUES BIGINT '20'");
        }
        assertThat(assertions.query("SELECT JSON_VALUE(JSON '[10,20,30]', 'strict $[$i]' PASSING NUMBER '1.5' AS \"i\" RETURNING BIGINT ERROR ON ERROR)"))
                .matches("VALUES BIGINT '30'");
        assertThat(assertions.query(
                """
                SELECT JSON_QUERY(JSON '[10,20,30]', 'strict $[$i to $j]'
                    PASSING NUMBER '1.4' AS "i", NUMBER '2.4' AS "j"
                    WITH UNCONDITIONAL ARRAY WRAPPER ERROR ON ERROR)
                """))
                .matches("VALUES JSON '[20,30]'");
    }

    @Test
    void testNumberArraySubscriptErrors()
    {
        for (String index : List.of("NaN", "+Infinity", "-Infinity", "9223372036854775808", "9223372036854775807", "-0.5", "3")) {
            String query = "SELECT JSON_VALUE(JSON '[10,20,30]', 'strict $[$i]' PASSING NUMBER '" + index + "' AS \"i\" RETURNING BIGINT %s ON ERROR)";
            assertThat(assertions.query(query.formatted("NULL")))
                    .matches("VALUES CAST(NULL AS BIGINT)");
            assertThat(assertions.query(query.formatted("ERROR")))
                    .failure()
                    .hasErrorCode(PATH_EVALUATION_ERROR);
        }
    }
}
