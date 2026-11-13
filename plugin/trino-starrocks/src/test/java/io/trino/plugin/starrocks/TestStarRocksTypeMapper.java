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
package io.trino.plugin.starrocks;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStarRocksTypeMapper
{
    private final StarRocksTypeMapper mapper = new StarRocksTypeMapper(TESTING_TYPE_MANAGER);

    @Test
    void testPrimitiveMappings()
    {
        assertThat(mapper.toTrinoType(column("is_enabled", "TINYINT", 1, null, 1, "tinyint(1)"))).isEqualTo(TINYINT);
        assertThat(mapper.toTrinoType(column("flag", "BOOLEAN", null, null, 2))).isEqualTo(BOOLEAN);
        assertThat(mapper.toTrinoType(column("flag_alias", "BOOL", null, null, 2))).isEqualTo(BOOLEAN);
        assertThat(mapper.toTrinoType(column("retry_count", "TINYINT", 4, null, 3, "tinyint(4)"))).isEqualTo(TINYINT);
        assertThat(mapper.toTrinoType(column("retry_count_unsigned", "TINYINT", 4, null, 4, "tinyint(4) unsigned"))).isEqualTo(SMALLINT);
        assertThat(mapper.toTrinoType(column("user_id", "INT", 11, null, 4))).isEqualTo(INTEGER);
        assertThat(mapper.toTrinoType(column("user_id_unsigned", "INT", 11, null, 5, "int(11) unsigned"))).isEqualTo(BIGINT);
        assertThat(mapper.toTrinoType(column("total", "BIGINT", 20, null, 5))).isEqualTo(BIGINT);
        assertThat(mapper.toTrinoType(column("total_unsigned", "BIGINT UNSIGNED", 39, null, 6, "bigint(20) unsigned"))).isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("score", "FLOAT", null, null, 6))).isEqualTo(REAL);
        assertThat(mapper.toTrinoType(column("ratio", "DOUBLE", null, null, 7))).isEqualTo(DOUBLE);
    }

    @Test
    void testDecimalAndCharacterMappings()
    {
        assertThat(mapper.toTrinoType(column("amount", "DECIMAL", 18, 4, 1, "decimal(18,4)"))).isEqualTo(createDecimalType(18, 4));
        assertThat(mapper.toTrinoType(column("oversized_amount", "DECIMAL256", 76, 18, 2, "decimal256(76,18)"))).isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("code", "CHAR", 8, null, 3, "char(8)"))).isEqualTo(createCharType(8));
        assertThat(mapper.toTrinoType(column("name", "VARCHAR", 32, null, 4, "varchar(32)"))).isEqualTo(createVarcharType(32));
        assertThat(mapper.toTrinoType(column("description", "VARCHAR", null, null, 5))).isEqualTo(createUnboundedVarcharType());
    }

    @Test
    void testTemporalAndBinaryMappings()
    {
        assertThat(mapper.toTrinoType(column("event_date", "DATE", null, null, 1))).isEqualTo(DATE);
        assertThat(mapper.toTrinoType(column("created_at", "DATETIME", null, null, 2, "datetimev2(3)"))).isEqualTo(createTimestampType(3));
        assertThat(mapper.toTrinoType(column("updated_at", "TIMESTAMP", null, 6, 3))).isEqualTo(createTimestampType(6));
        assertThat(mapper.toTrinoType(column("payload", "VARBINARY", null, null, 4))).isEqualTo(VARBINARY);
    }

    @Test
    void testStarRocksSpecificAndUnknownTypesFallbackToVarchar()
    {
        assertThat(mapper.toTrinoType(column("json_payload", "JSON", null, null, 1, "json")).getBaseName()).isEqualTo(JSON);
        assertThat(mapper.toTrinoType(column("variant_payload", "VARIANT", null, null, 2, "variant")).getBaseName()).isEqualTo(JSON);
        assertThat(mapper.toTrinoType(column("large_id", "DECIMAL", null, null, 1, "largeint"))).isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("bitmap_summary", "BITMAP", null, null, 3, "bitmap"))).isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("mystery", "GEOGRAPHY", null, null, 5))).isEqualTo(createUnboundedVarcharType());
    }

    @Test
    void testComplexTypeMappings()
    {
        assertThat(mapper.toTrinoType(column("tags", "ARRAY", null, null, 1, "array<varchar(12)>")))
                .isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("scores", "MAP", null, null, 2, "map<varchar(8),array<int>>")))
                .isEqualTo(createUnboundedVarcharType());
        assertThat(mapper.toTrinoType(column("details", "STRUCT", null, null, 3, "struct<a:int,b:varchar(12),c:array<bigint>>")))
                .isEqualTo(createUnboundedVarcharType());
    }

    private static StarRocksRemoteColumn column(String name, String type, Integer size, Integer scale, int ordinalPosition)
    {
        return column(name, type, size, scale, ordinalPosition, null);
    }

    private static StarRocksRemoteColumn column(String name, String type, Integer size, Integer scale, int ordinalPosition, String typeDefinition)
    {
        return new StarRocksRemoteColumn(
                name,
                name,
                type,
                Optional.ofNullable(size),
                Optional.ofNullable(scale),
                ordinalPosition,
                Optional.ofNullable(typeDefinition));
    }
}
