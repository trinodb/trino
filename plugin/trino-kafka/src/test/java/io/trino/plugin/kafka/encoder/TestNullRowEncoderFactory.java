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
package io.trino.plugin.kafka.encoder;

import com.google.common.collect.ImmutableList;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.kafka.encoder.KafkaFieldType.KEY;
import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNullRowEncoderFactory
{
    private static final NullRowEncoderFactory FACTORY = new NullRowEncoderFactory();

    @Test
    public void testAbsentKeyIsNull()
            throws Exception
    {
        try (RowEncoder encoder = FACTORY.create(SESSION, spec(KEY))) {
            assertThat(encoder.toByteArray()).isNull();
            assertThat(encoder.toByteArray()).isNull();
        }
    }

    @Test
    public void testAbsentMessageIsNotWritten()
    {
        // writing a null message would write a tombstone
        assertThatThrownBy(() -> FACTORY.create(SESSION, spec(MESSAGE)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Inserting into the message of table 'orders' requires a schema registry subject");
    }

    @Test
    public void testColumnsAreRejected()
    {
        KafkaColumnHandle column = new KafkaColumnHandle("key", BIGINT, "key", null, null, true, false, false, Optional.empty());
        RowEncoderSpec spec = new RowEncoderSpec(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of(column), "orders", KEY);
        assertThatThrownBy(() -> FACTORY.create(SESSION, spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No columns are expected for the KEY of topic 'orders'");
    }

    private static RowEncoderSpec spec(KafkaFieldType kafkaFieldType)
    {
        return new RowEncoderSpec(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of(), "orders", kafkaFieldType);
    }
}
