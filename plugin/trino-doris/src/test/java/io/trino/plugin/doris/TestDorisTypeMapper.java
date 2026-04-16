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
package io.trino.plugin.doris;

import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDorisTypeMapper
{
    @Test
    void testPrimitiveMappings()
    {
        DorisTypeMapper mapper = new DorisTypeMapper(new DorisConfig());

        assertThat(mapper.toTrinoType(column("is_active", "tinyint", 0, null, 1))).isEqualTo(BOOLEAN);
        assertThat(mapper.toTrinoType(column("is_enabled", "tinyint", 1, null, 2))).isEqualTo(BOOLEAN);
        assertThat(mapper.toTrinoType(column("is_deleted", "tinyint", null, null, 3, "boolean"))).isEqualTo(BOOLEAN);
        assertThat(mapper.toTrinoType(column("is_visible", "tinyint", null, null, 4, "tinyint(1)"))).isEqualTo(BOOLEAN);
        assertThat(mapper.toTrinoType(column("retry_count", "tinyint", null, null, 4))).isEqualTo(TINYINT);
        assertThat(mapper.toTrinoType(column("tiny1_user_defined", "tinyint", 3, null, 5, "tinyint(4)"))).isEqualTo(TINYINT);
        assertThat(mapper.toTrinoType(column("user_id", "int", 11, null, 6))).isEqualTo(INTEGER);
        assertThat(mapper.toTrinoType(column("total", "bigint", 20, null, 7))).isEqualTo(BIGINT);
        assertThat(mapper.toTrinoType(column("unsigned_id", "bigint unsigned", 20, 0, 8))).isEqualTo(createDecimalType(20));
    }

    @Test
    void testDecimalAndLargeintMappings()
    {
        DorisTypeMapper defaultMapper = new DorisTypeMapper(new DorisConfig());
        DorisTypeMapper decimalLargeintMapper = new DorisTypeMapper(new DorisConfig()
                .setLargeintMapping(DorisLargeintMapping.DECIMAL));

        assertThat(defaultMapper.toTrinoType(column("amount", "decimal", 18, 4, 1))).isEqualTo(createDecimalType(18, 4));
        assertThat(defaultMapper.toTrinoType(column("amount32", "decimal32", 9, 2, 2))).isEqualTo(createDecimalType(9, 2));
        assertThat(defaultMapper.toTrinoType(column("amount_v2", "decimalv2", 18, 6, 3))).isEqualTo(createDecimalType(18, 6));
        assertThat(defaultMapper.toTrinoType(column("oversized_amount", "decimal256", 76, 18, 4))).isEqualTo(createUnboundedVarcharType());
        assertThat(defaultMapper.toTrinoType(column("order_key", "largeint", 39, 0, 5))).isEqualTo(createUnboundedVarcharType());
        assertThat(decimalLargeintMapper.toTrinoType(column("order_key", "largeint", 39, 0, 5))).isEqualTo(createDecimalType(38));
    }

    @Test
    void testUsesTypeDefinitionWhenDriverDataTypeIsGeneric()
    {
        DorisTypeMapper mapper = new DorisTypeMapper(new DorisConfig());

        assertThat(mapper.toTrinoType(column("large_id", "decimal", null, null, 1, "largeint"))).isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("created_at", "datetime", null, null, 2, "datetimev2(3)"))).isEqualTo(createTimestampType(3));
        assertThat(mapper.toTrinoType(column("created_at", "datetime", null, null, 3, "datetime(3)"))).isEqualTo(createTimestampType(3));
        assertThat(mapper.toTrinoType(column("amount", "decimal", null, null, 4, "decimal(18, 4)"))).isEqualTo(createDecimalType(18, 4));
    }

    @Test
    void testCharacterTemporalAndFallbackMappings()
    {
        DorisTypeMapper mapper = new DorisTypeMapper(new DorisConfig());

        assertThat(mapper.toTrinoType(column("code", "char", 8, null, 1))).isEqualTo(createCharType(8));
        assertThat(mapper.toTrinoType(column("oversized_code", "char", CharType.MAX_LENGTH + 1, null, 2))).isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("name", "varchar", 32, null, 3))).isEqualTo(createVarcharType(32));
        assertThat(mapper.toTrinoType(column("created_on", "date_v2", null, null, 4))).isEqualTo(DATE);
        assertThat(mapper.toTrinoType(column("legacy_created_at", "datetime", null, 6, 5))).isEqualTo(createTimestampType(0));
        assertThat(mapper.toTrinoType(column("created_at", "datetime_v2", null, 6, 6))).isEqualTo(createTimestampType(6));
        assertThat(mapper.toTrinoType(column("payload", "json", null, null, 7))).isEqualTo(createUnboundedVarcharType());
    }

    @Test
    void testComplexTypesFailFastWithNormalizedTypeDefinitions()
    {
        DorisTypeMapper mapper = new DorisTypeMapper(new DorisConfig());

        assertThatThrownBy(() -> mapper.toTrinoType(column("arr_int_col", "array", null, null, 1, "array<decimal(18,2)>")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("array<decimal(18,2)>")
                .hasMessageContaining("arr_int_col");
        assertThatThrownBy(() -> mapper.toTrinoType(column("struct_col", "struct", null, null, 2, "struct<city:varchar(32), score:int>")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("struct<city:varchar(32), score:int>")
                .hasMessageContaining("struct_col");
    }

    @Test
    void testUnsupportedTypeFailsFast()
    {
        DorisTypeMapper mapper = new DorisTypeMapper(new DorisConfig());

        assertThatThrownBy(() -> mapper.toTrinoType(column("mystery", "geography", null, null, 1)))
                .isInstanceOf(TrinoException.class);
    }

    private static DorisRemoteColumn column(String name, String type, Integer size, Integer scale, int ordinalPosition)
    {
        return column(name, type, size, scale, ordinalPosition, null);
    }

    private static DorisRemoteColumn column(String name, String type, Integer size, Integer scale, int ordinalPosition, String typeDefinition)
    {
        return new DorisRemoteColumn(
                name,
                type,
                Optional.ofNullable(size),
                Optional.ofNullable(scale),
                ordinalPosition,
                Optional.ofNullable(typeDefinition));
    }
}
