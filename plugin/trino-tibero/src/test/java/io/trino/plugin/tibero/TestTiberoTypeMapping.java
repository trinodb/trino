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
package io.trino.plugin.tibero;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Types;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Type mapping tests for Tibero connector.
 * This class tests the mapping between Tibero database types and Trino types.
 */
public class TestTiberoTypeMapping
{
    private static final TiberoClient CLIENT = new TiberoClient(
            new BaseJdbcConfig(),
            session -> {
                throw new UnsupportedOperationException();
            },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;
    private static final Connection CONNECTION = null; // Not needed for these tests

    @Test
    public void testSmallintMapping()
    {
        assertThat(toTrinoType(Types.SMALLINT, "smallint"))
                .hasValueSatisfying(mapping -> assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.SmallintType.SMALLINT));
    }

    @Test
    public void testIntegerMapping()
    {
        assertThat(toTrinoType(Types.INTEGER, "integer"))
                .hasValueSatisfying(mapping -> assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.IntegerType.INTEGER));
    }

    @Test
    public void testBigintMapping()
    {
        assertThat(toTrinoType(Types.BIGINT, "bigint"))
                .hasValueSatisfying(mapping -> assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.BigintType.BIGINT));
    }

    @Test
    public void testRealMapping()
    {
        assertThat(toTrinoType(Types.REAL, "real"))
                .hasValueSatisfying(mapping -> assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.RealType.REAL));
    }

    @Test
    public void testDoubleMapping()
    {
        assertThat(toTrinoType(Types.DOUBLE, "double precision"))
                .hasValueSatisfying(mapping -> assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.DoubleType.DOUBLE));
    }

    @Test
    public void testCharMapping()
    {
        // CHAR(10)
        assertThat(toTrinoType(Types.CHAR, "char", 10, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.CharType.createCharType(10));
                });

        // CHAR(255)
        assertThat(toTrinoType(Types.CHAR, "char", 255, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.CharType.createCharType(255));
                });
    }

    @Test
    public void testVarcharMapping()
    {
        // VARCHAR(10)
        assertThat(toTrinoType(Types.VARCHAR, "varchar", 10, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarcharType.createVarcharType(10));
                });

        // VARCHAR(255)
        assertThat(toTrinoType(Types.VARCHAR, "varchar", 255, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarcharType.createVarcharType(255));
                });

        // VARCHAR(65535)
        assertThat(toTrinoType(Types.VARCHAR, "varchar", 65535, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarcharType.createVarcharType(65535));
                });
    }

    @Test
    public void testNvarcharMapping()
    {
        // NVARCHAR(10)
        assertThat(toTrinoType(Types.NVARCHAR, "nvarchar", 10, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarcharType.createVarcharType(10));
                });
    }

    @Test
    public void testDecimalMapping()
    {
        // DECIMAL(3, 0)
        assertThat(toTrinoType(Types.DECIMAL, "decimal", 3, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.DecimalType.createDecimalType(3, 0));
                });

        // DECIMAL(10, 2)
        assertThat(toTrinoType(Types.DECIMAL, "decimal", 10, 2))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.DecimalType.createDecimalType(10, 2));
                });

        // DECIMAL(38, 0)
        assertThat(toTrinoType(Types.DECIMAL, "decimal", 38, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.DecimalType.createDecimalType(38, 0));
                });

        // DECIMAL(38, 38)
        assertThat(toTrinoType(Types.DECIMAL, "decimal", 38, 38))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.DecimalType.createDecimalType(38, 38));
                });
    }

    @Test
    public void testNumericMapping()
    {
        // NUMERIC is same as DECIMAL in Tibero
        assertThat(toTrinoType(Types.NUMERIC, "numeric", 10, 2))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.DecimalType.createDecimalType(10, 2));
                });
    }

    @Test
    public void testDateMapping()
    {
        // Tibero DATE maps to TIMESTAMP(0) in Trino
        assertThat(toTrinoType("date"))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS);
                });
    }

    @Test
    public void testTimestampMapping()
    {
        // TIMESTAMP(0)
        assertThat(toTrinoType(Types.TIMESTAMP, "timestamp", 0, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.TimestampType.createTimestampType(0));
                });

        // TIMESTAMP(3)
        assertThat(toTrinoType(Types.TIMESTAMP, "timestamp", 0, 3))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.TimestampType.createTimestampType(3));
                });

        // TIMESTAMP(6)
        assertThat(toTrinoType(Types.TIMESTAMP, "timestamp", 0, 6))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.TimestampType.createTimestampType(6));
                });

        // TIMESTAMP(9)
        assertThat(toTrinoType(Types.TIMESTAMP, "timestamp", 0, 9))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.TimestampType.createTimestampType(9));
                });
    }

    @Test
    public void testVarbinaryMapping()
    {
        // Tibero RAW(n) maps to VARBINARY
        assertThat(toTrinoType(Types.VARBINARY, "raw", 100, 0))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarbinaryType.VARBINARY);
                });
    }

    @Test
    public void testBlobMapping()
    {
        // BLOB maps to VARBINARY
        assertThat(toTrinoType(Types.BLOB, "blob"))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarbinaryType.VARBINARY);
                });
    }

    @Test
    public void testClobMapping()
    {
        // CLOB maps to unbounded VARCHAR
        assertThat(toTrinoType(Types.CLOB, "clob"))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarcharType.createUnboundedVarcharType());
                });
    }

    @Test
    public void testNclobMapping()
    {
        // NCLOB maps to unbounded VARCHAR
        assertThat(toTrinoType(Types.NCLOB, "nclob"))
                .hasValueSatisfying(mapping -> {
                    assertThat(mapping.getType()).isEqualTo(io.trino.spi.type.VarcharType.createUnboundedVarcharType());
                });
    }

    private Optional<ColumnMapping> toTrinoType(int jdbcType, String jdbcTypeName)
    {
        return toTrinoType(jdbcType, jdbcTypeName, 0, 0);
    }

    private Optional<ColumnMapping> toTrinoType(String jdbcTypeName)
    {
        return CLIENT.toColumnMapping(SESSION, CONNECTION, new JdbcTypeHandle(
                Types.OTHER,
                Optional.of(jdbcTypeName),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    private Optional<ColumnMapping> toTrinoType(int jdbcType, String jdbcTypeName, int columnSize, int decimalDigits)
    {
        return CLIENT.toColumnMapping(SESSION, CONNECTION, new JdbcTypeHandle(
                jdbcType,
                Optional.of(jdbcTypeName),
                Optional.of(columnSize),
                Optional.of(decimalDigits),
                Optional.empty(),
                Optional.empty()));
    }
}
