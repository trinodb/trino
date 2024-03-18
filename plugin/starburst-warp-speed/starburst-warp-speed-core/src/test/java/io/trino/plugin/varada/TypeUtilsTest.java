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
package io.trino.plugin.varada;

import io.trino.plugin.varada.type.TypeUtils;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TypeUtilsTest
{
    static Stream<Arguments> strTypes()
    {
        return Stream.of(
                arguments(CharType.createCharType(2), true),
                arguments(VarcharType.createVarcharType(2), true),
                arguments(new ArrayType(DoubleType.DOUBLE), true),
                arguments(new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators()), false),
                arguments(IntegerType.INTEGER, false),
                arguments(null, false));
    }

    static Stream<Arguments> warmBasicSupportedTypes()
    {
        return Stream.of(
                arguments(CharType.createCharType(2), true),
                arguments(VarcharType.createVarcharType(2), true),
                arguments(IntegerType.INTEGER, true),
                arguments(RowType.rowType(RowType.field(VarcharType.VARCHAR)), false),
                arguments(new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators()), true),
                arguments(new ArrayType(DoubleType.DOUBLE), false),
                arguments(null, false));
    }

    static Stream<Arguments> warmDataSupportedTypes()
    {
        return Stream.of(
                arguments(CharType.createCharType(2), true),
                arguments(VarcharType.createVarcharType(2), true),
                arguments(IntegerType.INTEGER, true),
                arguments(new ArrayType(DoubleType.DOUBLE), true),
                arguments(RowType.rowType(RowType.field(VarcharType.VARCHAR)), false),
                arguments(new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators()), false),
                arguments(new ArrayType(RowType.rowType(RowType.field(VarcharType.VARCHAR))), false),
                arguments(null, false));
    }

    static Stream<Arguments> warmLuceneSupportedTypes()
    {
        return Stream.of(
                arguments(CharType.createCharType(2), true),
                arguments(VarcharType.createVarcharType(2), true),
                arguments(IntegerType.INTEGER, false),
                arguments(new ArrayType(CharType.createCharType(2)), true),
                arguments(new ArrayType(VarcharType.createVarcharType(2)), true),
                arguments(RowType.rowType(RowType.field(VarcharType.VARCHAR)), false),
                arguments(new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators()), false),
                arguments(new ArrayType(RowType.rowType(RowType.field(VarcharType.VARCHAR))), false),
                arguments(null, false));
    }

    @ParameterizedTest
    @MethodSource("strTypes")
    public void testIsStrType(Type type, boolean expected)
    {
        assertThat(TypeUtils.isStrType(type)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("warmBasicSupportedTypes")
    public void testIsWarmBasicSupported(Type type, boolean expected)
    {
        assertThat(TypeUtils.isWarmBasicSupported(type)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("warmDataSupportedTypes")
    public void testIsWarmDataSupported(Type type, boolean expected)
    {
        assertThat(TypeUtils.isWarmDataSupported(type)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("warmLuceneSupportedTypes")
    public void testIsWarmLuceneSupported(Type type, boolean expected)
    {
        assertThat(TypeUtils.isWarmLuceneSupported(type)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("warmBasicSupportedTypes")
    public void testIsWarmBloomSupported(Type type, boolean expected)
    {
        assertThat(TypeUtils.isWarmBloomSupported(type)).isEqualTo(expected);
    }
}
