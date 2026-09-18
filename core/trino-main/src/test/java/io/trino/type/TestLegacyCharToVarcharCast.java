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
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLegacyCharToVarcharCast
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        // With deprecated.legacy-varchar-to-char-coercion set, the legacy CHAR -> VARCHAR cast operator is registered
        // in place of the default (trimming) one, restoring the previous space-padding behavior.
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
    public void testCharToVarcharCastRepadsToCharLength()
    {
        // widening: the value is re-padded to the source CHAR length, not returned unpadded
        assertThat(assertions.expression("cast(a as varchar(10))")
                .binding("a", "CAST('bar' AS char(5))"))
                .hasType(createVarcharType(10))
                .isEqualTo("bar  ");

        // narrowing: truncated to the target length and padded to it
        assertThat(assertions.expression("cast(a as varchar(4))")
                .binding("a", "CAST('bar' AS char(5))"))
                .hasType(createVarcharType(4))
                .isEqualTo("bar ");
    }
}
