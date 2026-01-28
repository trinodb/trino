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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRowReplaceFunction
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testRowReplaceVarchar()
    {
        assertThat(assertions.expression("row_replace(data, 'location.planet', 'earth')")
                .binding("data", "CAST(row('hello', ROW(1, 'world')) AS row(greeting varchar, location row(id integer, planet varchar)))"))
                .hasType(rowType(field("greeting", VARCHAR), field("location", rowType(field("id", INTEGER), field("planet", VARCHAR)))))
                .isEqualTo(ImmutableList.of("hello", ImmutableList.of(1, "earth")));
    }

    @Test
    public void testRowReplaceInteger()
    {
        assertThat(assertions.expression("row_replace(data, 'location.id', 2)")
                .binding("data", "CAST(row('hello', ROW(1, 'world')) AS row(greeting varchar, location row(id integer, planet varchar)))"))
                .hasType(rowType(field("greeting", VARCHAR), field("location", rowType(field("id", INTEGER), field("planet", VARCHAR)))))
                .isEqualTo(ImmutableList.of("hello", ImmutableList.of(2, "world")));
    }
}
