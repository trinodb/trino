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
package io.trino.plugin.ydb;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.spi.type.Int128;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.ydb.jdbc.YdbConst.SQL_KIND_DECIMAL;
import static tech.ydb.jdbc.YdbConst.SQL_KIND_PRIMITIVE;

public class TestYdbColumnMappings
{
    @Test
    public void testUnsignedBigintWriter()
            throws Exception
    {
        List<List<Object>> calls = new ArrayList<>();
        PreparedStatement statement = statement(calls);
        ObjectWriteFunction writer = (ObjectWriteFunction) YdbColumnMappings.unsignedBigintColumnMapping().getWriteFunction();
        writer.set(statement, 1, Int128.valueOf(new BigInteger("18446744073709551615")));
        writer.setNull(statement, 2);
        assertThat(calls).containsExactly(List.of(1, -1L, SQL_KIND_PRIMITIVE + 8), List.of(2, SQL_KIND_PRIMITIVE + 8));
        assertThatThrownBy(() -> writer.set(statement, 1, Int128.valueOf(-1))).isInstanceOf(java.sql.SQLDataException.class);
        assertThatThrownBy(() -> writer.set(statement, 1, Int128.valueOf(BigInteger.ONE.shiftLeft(64)))).isInstanceOf(java.sql.SQLDataException.class);
    }

    @Test
    public void testDecimalWriterPreservesPrecisionAndScale()
            throws Exception
    {
        List<List<Object>> calls = new ArrayList<>();
        PreparedStatement statement = statement(calls);
        LongWriteFunction shortWriter = (LongWriteFunction) YdbColumnMappings.decimalColumnMapping(createDecimalType(10, 3)).getWriteFunction();
        shortWriter.set(statement, 1, 12345);
        ObjectWriteFunction longWriter = (ObjectWriteFunction) YdbColumnMappings.decimalColumnMapping(createDecimalType(35, 10)).getWriteFunction();
        longWriter.set(statement, 2, Int128.valueOf(new BigInteger("123456789012345678901234567890")));
        longWriter.setNull(statement, 3);
        assertThat(calls).containsExactly(
                List.of(1, new BigDecimal("12.345"), SQL_KIND_DECIMAL + (10 << 6) + 3),
                List.of(2, new BigDecimal("12345678901234567890.1234567890"), SQL_KIND_DECIMAL + (35 << 6) + 10),
                List.of(3, SQL_KIND_DECIMAL + (35 << 6) + 10));
    }

    @Test
    public void testUtcTimestampReads()
            throws Exception
    {
        for (String type : List.of("Datetime", "Datetime64", "Timestamp", "Timestamp64")) {
            ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class},
                    (_, method, args) -> {
                        assertThat(method.getName()).isEqualTo("getObject");
                        if (type.startsWith("Datetime")) {
                            assertThat(args[1]).isEqualTo(LocalDateTime.class);
                            return LocalDateTime.parse("2020-02-29T12:34:56");
                        }
                        assertThat(args[1]).isEqualTo(Instant.class);
                        return Instant.parse("2020-02-29T12:34:56Z");
                    });
            JdbcTypeHandle handle = new JdbcTypeHandle(Types.TIMESTAMP, Optional.of(type), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
            LongReadFunction reader = (LongReadFunction) YdbColumnMappings.timestampColumnMapping(handle).getReadFunction();
            assertThat(reader.readLong(resultSet, 1)).isEqualTo(Instant.parse("2020-02-29T12:34:56Z").getEpochSecond() * 1_000_000);
        }
    }

    private static PreparedStatement statement(List<List<Object>> calls)
    {
        return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class},
                (_, method, args) -> {
                    assertThat(method.getName()).isIn("setObject", "setNull");
                    calls.add(Arrays.asList(args));
                    return null;
                });
    }
}
