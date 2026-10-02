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
package io.trino.jsonpath;

import io.trino.sql.query.QueryAssertions;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLegacyCharJsonPathPredicate
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        // With the legacy coercion enabled, the padding char -> varchar cast is registered in
        // place of the default trimming one. The path language converts char parameters through
        // whichever cast is registered, so it pads here too.
        assertions = new QueryAssertions(new StandaloneQueryRunner(
                testSessionBuilder().build(),
                builder -> builder.addProperty("deprecated.legacy-varchar-to-char-coercion", "true")));
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCharacterParameterIsPadded()
    {
        // Under legacy semantics char(5) 'ab' converts to 'ab   ', so it is not a prefix of "abc".
        assertThat(assertions.query(
                "SELECT json_exists('{\"s\":\"abc\"}', 'lax $.s ? (@ starts with $prefix)' PASSING CAST('ab' AS char(5)) AS \"prefix\")"))
                .matches("VALUES false");

        // ... and it does match a value that carries the padding.
        assertThat(assertions.query(
                "SELECT json_exists('{\"s\":\"ab   c\"}', 'lax $.s ? (@ starts with $prefix)' PASSING CAST('ab' AS char(5)) AS \"prefix\")"))
                .matches("VALUES true");
    }
}
