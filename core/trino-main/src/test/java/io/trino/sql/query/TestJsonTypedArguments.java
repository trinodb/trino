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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestJsonTypedArguments
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testArrayArguments()
    {
        assertThat(assertions.query("SELECT json_array(JSON '1', JSON '\"text\"', JSON '{\"a\":[1]}', JSON 'null')"))
                .matches("VALUES VARCHAR '[1,\"text\",{\"a\":[1]},null]'");
        assertThat(assertions.query("SELECT json_array(j) FROM (VALUES JSON '[1,2]') t(j)"))
                .matches("VALUES VARCHAR '[[1,2]]'");
        assertThat(assertions.query("SELECT json_array(CAST(JSON '1' AS VARCHAR))"))
                .matches("VALUES VARCHAR '[\"1\"]'");
    }

    @Test
    void testObjectArguments()
    {
        assertThat(assertions.query("SELECT json_object('n': JSON '1', 's': JSON '\"text\"', 'a': JSON '[1,2]', 'o': JSON '{\"x\":true}', 'z': JSON 'null')"))
                .matches("VALUES VARCHAR '{\"n\":1,\"s\":\"text\",\"a\":[1,2],\"o\":{\"x\":true},\"z\":null}'");
        assertThat(assertions.query("SELECT json_object('v': j) FROM (VALUES JSON '{\"x\":1}') t(j)"))
                .matches("VALUES VARCHAR '{\"v\":{\"x\":1}}'");
    }

    @Test
    void testPassingArguments()
    {
        assertThat(assertions.query("SELECT json_query('{}', 'strict $p.a[0]' PASSING JSON '{\"a\":[1,2]}' AS \"p\")"))
                .matches("VALUES VARCHAR '1'");
        assertThat(assertions.query("SELECT json_query('{}', 'strict $p[1]' PASSING j AS \"p\") FROM (VALUES JSON '[1,2]') t(j)"))
                .matches("VALUES VARCHAR '2'");
        assertThat(assertions.query("SELECT json_query('{}', 'strict $p' PASSING JSON 'true' AS \"p\")"))
                .matches("VALUES VARCHAR 'true'");
        assertThat(assertions.query("SELECT json_query('{}', 'strict $p' PASSING JSON 'null' AS \"p\")"))
                .matches("VALUES VARCHAR 'null'");
        assertThat(assertions.query("SELECT json_query('{}', 'strict $p' PASSING JSON '\"text\"' AS \"p\")"))
                .matches("VALUES VARCHAR '\"text\"'");
    }
}
