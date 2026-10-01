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
package io.trino.plugin.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestJsonTransportCodec
{
    private static final ArrayType BOOLEAN_ARRAY = new ArrayType(BOOLEAN);

    @Test
    void testBooleanTransportAcceptsCanonicalValues()
            throws SQLException
    {
        Block block = JsonTransportCodec.readJsonArray(resultSet("[\"true\", \"false\"]"), 1, BOOLEAN_ARRAY);

        assertThat(BOOLEAN.getBoolean(block, 0)).isTrue();
        assertThat(BOOLEAN.getBoolean(block, 1)).isFalse();
    }

    @Test
    void testBooleanTransportRejectsNonCanonicalValue()
    {
        assertThatThrownBy(() -> JsonTransportCodec.readJsonArray(resultSet("[\"TRUE\"]"), 1, BOOLEAN_ARRAY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(JDBC_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Invalid boolean JSON transport value: TRUE");
                });
    }

    private static ResultSet resultSet(String value)
    {
        return (ResultSet) Proxy.newProxyInstance(
                TestJsonTransportCodec.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (_, method, _) -> {
                    if (method.getName().equals("getString")) {
                        return value;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
