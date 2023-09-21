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
package io.trino.plugin.kafka.encoder.raw;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;

public class TestRawEncoderMapping
{
    private static final RawRowEncoderFactory ENCODER_FACTORY = new RawRowEncoderFactory();
    private static final String TOPIC = "topic";

    @Test
    public void testMapping()
    {
        EncoderColumnHandle col1 = new KafkaColumnHandle("test1", BIGINT, "0", "LONG", null, false, false, false);
        EncoderColumnHandle col2 = new KafkaColumnHandle("test2", createUnboundedVarcharType(), "8:14", "BYTE", null, false, false, false);
        EncoderColumnHandle col3 = new KafkaColumnHandle("test3", BIGINT, "14", "LONG", null, false, false, false);
        EncoderColumnHandle col4 = new KafkaColumnHandle("test4", createUnboundedVarcharType(), "22:28", "BYTE", null, false, false, false);
        EncoderColumnHandle col5 = new KafkaColumnHandle("test5", BIGINT, "28", "LONG", null, false, false, false);
        EncoderColumnHandle col6 = new KafkaColumnHandle("test6", createVarcharType(6), "36:42", "BYTE", null, false, false, false);
        EncoderColumnHandle col7 = new KafkaColumnHandle("test7", createVarcharType(6), "42:48", "BYTE", null, false, false, false);

        RowEncoder rowEncoder = ENCODER_FACTORY.create(TestingConnectorSession.SESSION, new RowEncoderSpec(RawRowEncoder.NAME, Optional.empty(), ImmutableList.of(col1, col2, col3, col4, col5, col6, col7), TOPIC, MESSAGE));

        ByteBuffer buf = ByteBuffer.allocate(48);
        buf.putLong(123456789); // 0-8
        buf.put("abcdef".getBytes(StandardCharsets.UTF_8)); // 8-14
        buf.putLong(123456789); // 14-22
        buf.put("abcdef".getBytes(StandardCharsets.UTF_8)); // 22-28
        buf.putLong(123456789); // 28-36
        buf.put("abcdef".getBytes(StandardCharsets.UTF_8)); // 36-42
        buf.put("abcdef".getBytes(StandardCharsets.UTF_8)); // 42-48

        LongArrayBlockBuilder longArrayBlockBuilder = new LongArrayBlockBuilder(null, 1);
        BIGINT.writeLong(longArrayBlockBuilder, 123456789);
        Block longArrayBlock = longArrayBlockBuilder.build();

        Block varArrayBlock = new VariableWidthBlockBuilder(null, 1, 6)
                .writeEntry(Slices.wrappedBuffer("abcdef".getBytes(StandardCharsets.UTF_8)), 0, 6)
                .build();

        rowEncoder.appendColumnValue(longArrayBlock, 0);
        rowEncoder.appendColumnValue(varArrayBlock, 0);
        rowEncoder.appendColumnValue(longArrayBlock, 0);
        rowEncoder.appendColumnValue(varArrayBlock, 0);
        rowEncoder.appendColumnValue(longArrayBlock, 0);
        rowEncoder.appendColumnValue(varArrayBlock, 0);
        rowEncoder.appendColumnValue(varArrayBlock, 0);

        assertEquals(buf.array(), rowEncoder.toByteArray());
    }
}
