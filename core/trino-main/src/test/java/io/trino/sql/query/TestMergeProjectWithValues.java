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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMergeProjectWithValues
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
    public void testMergeProjectWithValues()
    {
        assertThat(assertions.query("SELECT a, b + 1, 'x' FROM (VALUES (1, 10, true), (2, 20, false), (3, 30, true)) t(a, b, c)"))
                .matches("VALUES (1, 11, 'x'), (2, 21, 'x'), (3, 31, 'x')");

        assertThat(assertions.query("SELECT a, b + 1, 'x' FROM (VALUES (1, 10, true), (null, null, null), (3, 30, true)) t(a, b, c)"))
                .matches("VALUES (1, 11, 'x'), (null, null, 'x'), (3, 31, 'x')");

        assertThat(assertions.query("SELECT (SELECT a * 10 FROM (VALUES x) t(a)) FROM (VALUES 1, 2, 3) t2(x)"))
                .matches("VALUES 10, 20, 30");
    }
}
