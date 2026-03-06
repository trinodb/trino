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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.trino.spi.type.ArrayType.arrayType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRowTransformFunction
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
        closeQuietly(assertions);
        assertions = null;
    }

    @Test
    public void testInteger()
    {
        assertThat(assertions.expression("transform(a, 'greeting', 1337, greeting -> greeting * 2)")
                .binding("a", "CAST(ROW(2, 3) as ROW(greeting integer, planet integer))"))
                .hasType(rowType(field("greeting", INTEGER), field("planet", INTEGER)))
                .isEqualTo(ImmutableList.of(4, 3));
    }

    @Test
    public void testVarchar()
    {
        assertThat(assertions.expression("transform(a, 'greeting', '', greeting -> concat(greeting, ' or goodbye'))")
                .binding("a", "CAST(ROW('hello', 'world') as ROW(greeting varchar, planet varchar))"))
                .hasType(rowType(field("greeting", VARCHAR), field("planet", VARCHAR)))
                .isEqualTo(ImmutableList.of("hello or goodbye", "world"));
    }


    @Test
    public void testNullReturn()
    {
        List<String> expected = new ArrayList<>();
        expected.add(null);
        expected.add("world");

        assertThat(assertions.expression("transform(a, 'greeting', '', greeting -> NULL)")
                .binding("a", "CAST(ROW('hello', 'world') as ROW(greeting varchar, planet varchar))"))
                .hasType(rowType(field("greeting", VARCHAR), field("planet", VARCHAR)))
                .isEqualTo(expected);
    }

    @Test
    public void testIntegerArray()
    {
        assertThat(assertions.expression("transform(a, 'greeting', ARRAY[0], greeting -> greeting || 2)")
                .binding("a", "CAST(ROW(ARRAY[1], 'world') as ROW(greeting array(integer), planet varchar))"))
                .hasType(rowType(field("greeting", new ArrayType(INTEGER)), field("planet", VARCHAR)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 2), "world"));
    }

    @Test
    public void testVarcharArray()
    {
        assertThat(assertions.expression("transform(a, 'greeting', ARRAY[''], greeting -> greeting || 'or' || 'goodbye')")
                .binding("a", "CAST(ROW(ARRAY['hello'], 'world') AS ROW(greeting array(varchar), planet varchar))"))
                .hasType(rowType(field("greeting", new ArrayType(VARCHAR)), field("planet", VARCHAR)))
                .isEqualTo(ImmutableList.of(ImmutableList.of("hello", "or", "goodbye"), "world"));
    }

    @Test
    public void testVarcharRowType()
    {
        assertThat(assertions.expression("transform(a, 'greeting', CAST(ROW('') as ROW(text varchar)), greeting -> transform(greeting, 'text', '', text -> concat(text, ' or goodbye')))")
                .binding("a", "CAST(ROW(ROW('hello'), 'world') as ROW(greeting ROW(text varchar), planet varchar))"))
                .hasType(rowType(field("greeting", rowType(field("text", VARCHAR))), field("planet", VARCHAR)))
                .isEqualTo(ImmutableList.of(ImmutableList.of("hello or goodbye"), "world"));
    }

    @Test
    public void testVarcharRowTypeArrayType()
    {
        assertThat(assertions.expression("""
                transform(a, data ->
                    transform(data, 'greeting', '', greeting -> concat(greeting, ' or goodbye')))
                """)
                .binding("a", """
                ARRAY[CAST(ROW('hello', 'world') as ROW(greeting varchar, planet varchar)),
                      CAST(ROW('hi', 'mars') as ROW(greeting varchar, planet varchar)),
                      CAST(ROW('hey', 'jupiter') as ROW(greeting varchar, planet varchar))]
                """))
                .hasType(arrayType(rowType(field("greeting", VARCHAR), field("planet", VARCHAR))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of("hello or goodbye", "world"),
                        ImmutableList.of("hi or goodbye", "mars"),
                        ImmutableList.of("hey or goodbye", "jupiter")));
    }
}
