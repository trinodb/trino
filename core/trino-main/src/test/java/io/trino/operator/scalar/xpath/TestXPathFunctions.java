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
package io.trino.operator.scalar.xpath;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public final class TestXPathFunctions
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
    public void testXPath()
    {
        Type arrayType = new ArrayType(VARCHAR);

        assertThat(assertions.function("xpath", "NULL", "'dummy'")).isNull(arrayType);
        assertThat(assertions.function("xpath", "'dummy'", "NULL")).isNull(arrayType);

        validateXPathExtract("xpath", "<a><b id=\"foo\">b1</b><b id=\"bar\">b2</b></a>", "//@id", arrayType,
                ImmutableList.of("foo", "bar"));
        validateXPathExtract("xpath", "<a><b id=\"1\"><c/></b><b id=\"2\"><c/></b></a>", "/descendant::c/ancestor::b/@id",
                arrayType, ImmutableList.of("1", "2"));
        validateXPathExtract("xpath", "<a><b>b1</b><b>b2</b></a>", "a/*", arrayType, ImmutableList.of());
        validateXPathExtract("xpath", "<a><b>b1</b><b>b2</b></a>", "a/*/text()", arrayType, ImmutableList.of("b1", "b2"));
        validateXPathExtract("xpath", "<a><b class=\"bb\">b1</b><b>b2</b><b>b3</b><c class=\"bb\">c1</c><c>c2</c></a>",
                "a/*[@class=\"bb\"]/text()", arrayType, ImmutableList.of("b1", "c1"));
    }

    @Test
    public void testXPathNode()
    {
        assertThat(assertions.function("xpath_node", "NULL", "'dummy'")).isNull(createVarcharType(0));
        assertThat(assertions.function("xpath_node", "'dummy'", "NULL")).isNull(createVarcharType(5));
        assertThat(assertions.function("xpath_node", "'<a><b>bb</b><c>cc</c></a>'", "'d'")).isNull(createVarcharType(25));
        assertThat(assertions.function("xpath_node", "'<a><b>b1</b><b>b2</b></a>'", "'//b'")).isNull(createVarcharType(25));

        validateXPathExtract("xpath_node", "<a><b><c>test</c></b></a>", "//text()", createVarcharType(25), "test");
        validateXPathExtract("xpath_node", "<a><b id=\"foo\">b1</b><b id=\"bar\">b2</b></a>", "//@id", createVarcharType(43), "foo");
        validateXPathExtract("xpath_node", "<a><b class=\"bb\">b1</b><b>b2</b><b>b3</b><c class=\"bb\">c1</c><c>c2</c></a>",
                "a/*[@class=\"bb\"]/text()", createVarcharType(74), "b1");
    }

    @Test
    public void testXPathString()
    {
        assertThat(assertions.function("xpath_string", "NULL", "'dummy'")).isNull(createVarcharType(0));
        assertThat(assertions.function("xpath_string", "'dummy'", "NULL")).isNull(createVarcharType(5));

        Type varcharType = createVarcharType(25);
        validateXPathExtract("xpath_string", "<a><b>bb</b><c>cc</c></a>", "a/b", varcharType, "bb");
        validateXPathExtract("xpath_string", "<a><b>bb</b><c>cc</c></a>", "a", varcharType, "bbcc");
        validateXPathExtract("xpath_string", "<a><b>bb</b><c>cc</c></a>", "a/d", varcharType, "");
        validateXPathExtract("xpath_string", "<a><b>bb</b><c>cc</c></a>", "d", varcharType, "");
        validateXPathExtract("xpath_string", "<a><b>b1</b><b>b2</b></a>", "//b", varcharType, "b1");
        validateXPathExtract("xpath_string", "<a><b>b1</b><b>b2</b></a>", "a/b[2]", varcharType, "b2");
        validateXPathExtract("xpath_string", "<a><b>b1</b><c>c1</c></a>", "concat(a/b[1], a/c)", varcharType, "b1c1");
        validateXPathExtract("xpath_string", "<a><b>b1</b><c>c1</c></a>", "a/b[1] + a/c", varcharType, "NaN");
        validateXPathExtract("xpath_string", "<a><b>b1</b><b id=\"b_2\">b2</b></a>", "a/b[@id=\"b_2\"]", createVarcharType(34), "b2");
    }

    @Test
    public void testXPathBoolean()
    {
        assertThat(assertions.function("xpath_boolean", "NULL", "'dummy'")).isNull(BOOLEAN);
        assertThat(assertions.function("xpath_boolean", "'dummy'", "NULL")).isNull(BOOLEAN);

        validateXPathExtract("xpath_boolean", "<a><b>b</b></a>", "a/b", BOOLEAN, true);
        validateXPathExtract("xpath_boolean", "<a><b>b</b></a>", "a/c", BOOLEAN, false);
        validateXPathExtract("xpath_boolean", "<a><b>b</b></a>", "a/b = \"b\"", BOOLEAN, true);
        validateXPathExtract("xpath_boolean", "<a><b>10</b></a>", "a/b < 10", BOOLEAN, false);
        validateXPathExtract("xpath_boolean", "<a><b>10</b></a>", "d", BOOLEAN, false);
        validateXPathExtract("xpath_boolean", "<a><b>2</b><c>3</c></a>", "a/b + a/c = 5", BOOLEAN, true);
    }

    @Test
    public void testXPathDouble()
    {
        assertThat(assertions.function("xpath_double", "NULL", "'dummy'")).isNull(DOUBLE);
        assertThat(assertions.function("xpath_double", "'dummy'", "NULL")).isNull(DOUBLE);

        validateXPathExtract("xpath_double", "<a>b</a>", "a = 10", DOUBLE, 0d);
        validateXPathExtract("xpath_double", "<a>this is not a number</a>", "a", DOUBLE, Double.NaN);
        validateXPathExtract("xpath_double", "<a>2000</a>", "d", DOUBLE, Double.NaN);
        validateXPathExtract("xpath_double", "<a><b>2000</b><c>not a number</c></a>", "a/b + a/c", DOUBLE, Double.NaN);
        validateXPathExtract("xpath_double", "<a><b>2000000000</b><c>40000000000</c></a>", "a/b * a/c", DOUBLE, 8e19d);
    }

    private void validateXPathExtract(String xpathFunctionName, String xml, String path, Type expectedType, Object expectedValue)
    {
        assertThat(assertions.function(xpathFunctionName, format("'%s'", xml), format("'%s'", path)))
                .hasType(expectedType)
                .isEqualTo(expectedValue);
    }
}
