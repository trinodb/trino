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
package io.prestosql.plugin.kafka.encoder.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.kafka.KafkaColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingConnectorSession;
import org.assertj.core.api.ThrowableAssert;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonEncoder
{
    private static final JsonRowEncoderFactory ENCODER_FACTORY = new JsonRowEncoderFactory(new ObjectMapper());

    private void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private void singleColumnEncoder(Type type)
    {
        ENCODER_FACTORY.create(TestingConnectorSession.SESSION, Optional.empty(), ImmutableList.of(new KafkaColumnHandle("default", type, "default", null, null, false, false, false)));
    }

    @Test
    public void testColumnValidation()
    {
        singleColumnEncoder(BIGINT);
        singleColumnEncoder(INTEGER);
        singleColumnEncoder(SMALLINT);
        singleColumnEncoder(TINYINT);
        singleColumnEncoder(DOUBLE);
        singleColumnEncoder(BOOLEAN);
        singleColumnEncoder(createVarcharType(20));
        singleColumnEncoder(createUnboundedVarcharType());

        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(REAL));
        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(createDecimalType(10, 4)));
        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(VARBINARY));
    }
}
