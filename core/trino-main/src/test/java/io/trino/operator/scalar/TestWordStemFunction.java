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

import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestWordStemFunction
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
    public void testWordStem()
    {
        assertThat(assertions.function("word_stem", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("word_stem", "'x'"))
                .hasType(createVarcharType(1))
                .isEqualTo("x");

        assertThat(assertions.function("word_stem", "'abc'"))
                .hasType(createVarcharType(3))
                .isEqualTo("abc");

        assertThat(assertions.function("word_stem", "'generally'"))
                .hasType(createVarcharType(9))
                .isEqualTo("general");

        assertThat(assertions.function("word_stem", "'useful'"))
                .hasType(createVarcharType(6))
                .isEqualTo("use");

        assertThat(assertions.function("word_stem", "'runs'"))
                .hasType(createVarcharType(4))
                .isEqualTo("run");

        assertThat(assertions.function("word_stem", "'run'"))
                .hasType(createVarcharType(3))
                .isEqualTo("run");

        assertThat(assertions.function("word_stem", "'authorized'", "'en'"))
                .hasType(createVarcharType(10))
                .isEqualTo("author");

        assertThat(assertions.function("word_stem", "'accessories'", "'en'"))
                .hasType(createVarcharType(11))
                .isEqualTo("accessori");

        assertThat(assertions.function("word_stem", "'intensifying'", "'en'"))
                .hasType(createVarcharType(12))
                .isEqualTo("intensifi");

        assertThat(assertions.function("word_stem", "'resentment'"))
                .hasType(createVarcharType(10))
                .isEqualTo("resent");

        assertThat(assertions.function("word_stem", "'faithfulness'"))
                .hasType(createVarcharType(12))
                .isEqualTo("faith");

        assertThat(assertions.function("word_stem", "'continuerait'", "'fr'"))
                .hasType(createVarcharType(12))
                .isEqualTo("continu");

        assertThat(assertions.function("word_stem", "'torpedearon'", "'es'"))
                .hasType(createVarcharType(11))
                .isEqualTo("torped");

        assertThat(assertions.function("word_stem", "'quilomtricos'", "'pt'"))
                .hasType(createVarcharType(12))
                .isEqualTo("quilomtr");

        assertThat(assertions.function("word_stem", "'pronunziare'", "'it'"))
                .hasType(createVarcharType(11))
                .isEqualTo("pronunz");

        assertThat(assertions.function("word_stem", "'auferstnde'", "'de'"))
                .hasType(createVarcharType(10))
                .isEqualTo("auferstnd");

        assertThat(assertions.function("word_stem", "'ã'", "'pt'"))
                .hasType(createVarcharType(1))
                .isEqualTo("ã");

        assertThat(assertions.function("word_stem", "'bastão'", "'pt'"))
                .hasType(createVarcharType(6))
                .isEqualTo("bastã");

        assertTrinoExceptionThrownBy(() -> assertions.function("word_stem", "'test'", "'xx'").evaluate())
                .hasMessage("Unknown stemmer language: xx");
    }
}
