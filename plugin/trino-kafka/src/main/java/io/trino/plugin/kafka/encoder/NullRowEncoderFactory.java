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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

/**
 * Encodes the absent key of a table that defines no key columns as a {@code null} Kafka key.
 */
public class NullRowEncoderFactory
        implements RowEncoderFactory
{
    private static final RowEncoder NULL_ROW_ENCODER = new RowEncoder()
    {
        @Override
        public void appendColumnValue(Block block, int position)
        {
            throw new IllegalStateException("No columns are defined");
        }

        @Override
        public byte[] toByteArray()
        {
            return null;
        }

        @Override
        public void close() {}
    };

    @Override
    public RowEncoder create(ConnectorSession session, RowEncoderSpec rowEncoderSpec)
    {
        checkArgument(rowEncoderSpec.columnHandles().isEmpty(), "No columns are expected for the %s of topic '%s'", rowEncoderSpec.kafkaFieldType(), rowEncoderSpec.topic());
        if (rowEncoderSpec.kafkaFieldType() == MESSAGE) {
            // a null message is a tombstone, which deletes the key from a compacted topic
            throw new TrinoException(NOT_SUPPORTED, format("Inserting into the message of table '%s' requires a schema registry subject", rowEncoderSpec.topic()));
        }
        return NULL_ROW_ENCODER;
    }
}
