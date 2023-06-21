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

import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.block.Block;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertTrue;

@TestInstance(PER_CLASS)
public class TestBlockAndPositionNullConvention
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalar(FunctionWithBlockAndPositionConvention.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testBlockPosition()
    {
        assertThat(assertions.function("test_block_position", "BIGINT '1234'"))
                .isEqualTo(1234L);
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionBigint.get());

        assertThat(assertions.function("test_block_position", "12.34e0"))
                .isEqualTo(12.34);
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionDouble.get());

        assertThat(assertions.function("test_block_position", "'hello'"))
                .hasType(createVarcharType(5))
                .isEqualTo("hello");
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionSlice.get());

        assertThat(assertions.function("test_block_position", "true"))
                .isEqualTo(true);
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionBoolean.get());
    }

    @ScalarFunction("test_block_position")
    public static final class FunctionWithBlockAndPositionConvention
    {
        private static final AtomicBoolean hitBlockPositionBigint = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionDouble = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionSlice = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionBoolean = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionObject = new AtomicBoolean();

        // generic implementations
        // these will not work right now because MethodHandle is not properly adapted

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Object object)
        {
            return object;
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @SqlNullable @BlockPosition @SqlType("E") Block block, @BlockIndex int position)
        {
            hitBlockPositionObject.set(true);
            return readNativeValue(type, block, position);
        }

        // specialized

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Slice specializedSlice(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Slice slice)
        {
            return slice;
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Slice specializedSlice(@TypeParameter("E") Type type, @SqlNullable @BlockPosition @SqlType(value = "E", nativeContainerType = Slice.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionSlice.set(true);
            return type.getSlice(block, position);
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Boolean speciailizedBoolean(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Boolean bool)
        {
            return bool;
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Boolean speciailizedBoolean(@TypeParameter("E") Type type, @SqlNullable @BlockPosition @SqlType(value = "E", nativeContainerType = boolean.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionBoolean.set(true);
            return type.getBoolean(block, position);
        }

        // exact

        @SqlType(StandardTypes.BIGINT)
        @SqlNullable
        public static Long getLong(@SqlNullable @SqlType(StandardTypes.BIGINT) Long number)
        {
            return number;
        }

        @SqlType(StandardTypes.BIGINT)
        @SqlNullable
        public static Long getBlockPosition(@SqlNullable @BlockPosition @SqlType(value = StandardTypes.BIGINT, nativeContainerType = long.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionBigint.set(true);
            return BIGINT.getLong(block, position);
        }

        @SqlType(StandardTypes.DOUBLE)
        @SqlNullable
        public static Double getDouble(@SqlNullable @SqlType(StandardTypes.DOUBLE) Double number)
        {
            return number;
        }

        @SqlType(StandardTypes.DOUBLE)
        @SqlNullable
        public static Double getDouble(@SqlNullable @BlockPosition @SqlType(value = StandardTypes.DOUBLE, nativeContainerType = double.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionDouble.set(true);
            return DOUBLE.getDouble(block, position);
        }
    }
}
