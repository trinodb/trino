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
package io.trino.plugin.teradata.functions;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTeradataFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new TeradataFunctionsPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testIndex()
    {
        assertThat(assertions.function("INDEX", "'high'", "'ig'"))
                .isEqualTo(2L);

        assertThat(assertions.function("INDEX", "'high'", "'igx'"))
                .isEqualTo(0L);

        assertThat(assertions.function("INDEX", "'Quadratically'", "'a'"))
                .isEqualTo(3L);

        assertThat(assertions.function("INDEX", "'foobar'", "'foobar'"))
                .isEqualTo(1L);

        assertThat(assertions.function("INDEX", "'foobar'", "'foobar_baz'"))
                .isEqualTo(0L);

        assertThat(assertions.function("INDEX", "'foobar'", "'obar'"))
                .isEqualTo(3L);

        assertThat(assertions.function("INDEX", "'zoo!'", "'!'"))
                .isEqualTo(4L);

        assertThat(assertions.function("INDEX", "'x'", "''"))
                .isEqualTo(1L);

        assertThat(assertions.function("INDEX", "''", "''"))
                .isEqualTo(1L);

        assertThat(assertions.function("INDEX", "NULL", "''"))
                .isNull(BIGINT);

        assertThat(assertions.function("INDEX", "''", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("INDEX", "NULL", "NULL"))
                .isNull(BIGINT);
    }

    @Test
    public void testChar2HexInt()
    {
        assertThat(assertions.function("CHAR2HEXINT", "'123'"))
                .hasType(VARCHAR)
                .isEqualTo("003100320033");

        assertThat(assertions.function("CHAR2HEXINT", "'One Ring'"))
                .hasType(VARCHAR)
                .isEqualTo("004F006E0065002000520069006E0067");
    }

    @Test
    public void testChar2HexIntUtf8()
    {
        assertThat(assertions.function("CHAR2HEXINT", "'\u0105'"))
                .hasType(VARCHAR)
                .isEqualTo("0105");

        assertThat(assertions.function("CHAR2HEXINT", "'\u0ca0'"))
                .hasType(VARCHAR)
                .isEqualTo("0CA0");

        assertThat(assertions.function("CHAR2HEXINT", "'\uff71'"))
                .hasType(VARCHAR)
                .isEqualTo("FF71");

        assertThat(assertions.function("CHAR2HEXINT", "'\u0ca0\u76ca\u0ca0'"))
                .hasType(VARCHAR)
                .isEqualTo("0CA076CA0CA0");

        assertThat(assertions.function("CHAR2HEXINT", "'(\u30ce\u0ca0\u76ca\u0ca0)\u30ce\u5f61\u253b\u2501\u253b'"))
                .hasType(VARCHAR)
                .isEqualTo("002830CE0CA076CA0CA0002930CE5F61253B2501253B");
    }
}
