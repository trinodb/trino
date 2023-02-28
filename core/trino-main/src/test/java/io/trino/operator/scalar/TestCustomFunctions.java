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

import io.trino.metadata.InternalFunctionBundle;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestCustomFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(CustomFunctions.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCustomAdd()
    {
        assertThat(assertions.function("custom_add", "123", "456"))
                .isEqualTo(579L);
    }

    @Test
    public void testSliceIsNull()
    {
        assertThat(assertions.function("custom_is_null", "CAST(NULL AS VARCHAR)"))
                .isEqualTo(true);

        assertThat(assertions.function("custom_is_null", "'not null'"))
                .isEqualTo(false);
    }

    @Test
    public void testLongIsNull()
    {
        assertThat(assertions.function("custom_is_null", "CAST(NULL AS BIGINT)"))
                .isEqualTo(true);

        assertThat(assertions.function("custom_is_null", "0"))
                .isEqualTo(false);
    }

    @Test
    public void testIdentityFunction()
    {
        assertThat(assertions.function("identity&function", "\"identity.function\"(123)"))
                .isEqualTo(123L);

        assertThat(assertions.function("identity.function", "\"identity&function\"(123)"))
                .isEqualTo(123L);
    }
}
