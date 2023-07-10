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
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestScalarParser
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalar(GenericWithIncompleteSpecializationNullable.class)
                .scalar(GenericWithIncompleteSpecializationNotNullable.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testGenericWithIncompleteSpecialization()
    {
        assertThat(assertions.function("generic_incomplete_specialization_nullable", "9876543210"))
                .isEqualTo(9876543210L);
        assertThat(assertions.function("generic_incomplete_specialization_nullable", "1.234E0"))
                .isEqualTo(1.234);
        assertThat(assertions.function("generic_incomplete_specialization_nullable", "'abcd'"))
                .hasType(createVarcharType(4))
                .isEqualTo("abcd");
        assertThat(assertions.function("generic_incomplete_specialization_nullable", "true"))
                .isEqualTo(true);
        assertThat(assertions.function("generic_incomplete_specialization_nullable", "array[1, 2]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("generic_incomplete_specialization_not_nullable", "9876543210"))
                .isEqualTo(9876543210L);
        assertThat(assertions.function("generic_incomplete_specialization_not_nullable", "1.234E0"))
                .isEqualTo(1.234);
        assertThat(assertions.function("generic_incomplete_specialization_not_nullable", "'abcd'"))
                .hasType(createVarcharType(4))
                .isEqualTo("abcd");
        assertThat(assertions.function("generic_incomplete_specialization_not_nullable", "true"))
                .isEqualTo(true);
        assertThat(assertions.function("generic_incomplete_specialization_not_nullable", "array[1, 2]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));
    }

    @ScalarFunction("generic_incomplete_specialization_nullable")
    public static final class GenericWithIncompleteSpecializationNullable
    {
        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Object input)
        {
            return input;
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Long specializedSlice(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Long input)
        {
            return input;
        }
    }

    @ScalarFunction("generic_incomplete_specialization_not_nullable")
    public static final class GenericWithIncompleteSpecializationNotNullable
    {
        @TypeParameter("E")
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @SqlType("E") Object input)
        {
            return input;
        }

        @TypeParameter("E")
        @SqlType("E")
        public static long specializedSlice(@TypeParameter("E") Type type, @SqlType("E") long input)
        {
            return input;
        }
    }
}
