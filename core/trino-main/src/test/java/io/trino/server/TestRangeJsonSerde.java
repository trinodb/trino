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
package io.trino.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestRangeJsonSerde
{
    @Test
    public void testRangeJsonSerde()
            throws JsonProcessingException
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addSerializer(Range.class, new RangeJsonSerde.Serializer())
                        .addDeserializer(Range.class, new RangeJsonSerde.Deserializer())
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        Range singleValueRange = Range.equal(BIGINT, 1L);
        Range range = Range.range(BIGINT, 12L, true, 42L, true);
        Range unboundedRange = Range.greaterThan(BIGINT, 123L);
        String serializedRanges = mapper.writeValueAsString(ImmutableList.of(singleValueRange, range, unboundedRange, singleValueRange));
        assertEquals(serializedRanges, "[" +
                "1,\"bigint\",\"CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA\"," +
                "0,{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAAAwAAAAAAAAA\",\"bound\":\"EXACTLY\"},{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAACoAAAAAAAAA\",\"bound\":\"EXACTLY\"}," +
                "0,{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAAHsAAAAAAAAA\",\"bound\":\"ABOVE\"},{\"type\":\"bigint\",\"bound\":\"BELOW\"}," +
                "1,\"bigint\",\"CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA\"]");
        assertEquals(mapper.readValue(serializedRanges, new TypeReference<List<Range>>() {}), ImmutableList.of(singleValueRange, range, unboundedRange, singleValueRange));
    }
}
