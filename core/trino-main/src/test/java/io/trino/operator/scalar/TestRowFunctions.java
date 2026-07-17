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

import java.util.Arrays;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRowFunctions
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
    public void testFields()
    {
        assertThat(assertions.expression("ROW::fields(data)")
                .binding("data", "CAST(ROW('hello', 'world') AS ROW(greeting VARCHAR, planet VARCHAR))"))
                .hasType(new ArrayType(VARCHAR))
                .neverFails()
                .isEqualTo(ImmutableList.of("greeting", "planet"));

        assertThat(assertions.expression("ROW::fields(data)")
                .binding("data", "CAST(NULL AS ROW(greeting VARCHAR, planet VARCHAR))"))
                .hasType(new ArrayType(VARCHAR))
                .neverFails()
                .isEqualTo(ImmutableList.of("greeting", "planet"));

        assertThat(assertions.expression("ROW::fields(data)")
                .binding("data", "ROW('hello', 'world')"))
                .hasType(new ArrayType(VARCHAR))
                .neverFails()
                .isEqualTo(Arrays.asList(null, null));

        assertThat(assertions.expression("ROW::fields(data)")
                .binding("data", "CAST(NULL AS ROW(greeting VARCHAR, planet ROW(color varchar, size bigint)))"))
                .hasType(new ArrayType(VARCHAR))
                .neverFails()
                .isEqualTo(ImmutableList.of("greeting", "planet"));
    }
}
