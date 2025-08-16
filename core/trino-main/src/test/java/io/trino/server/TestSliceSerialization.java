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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSliceSerialization
{
    private ObjectMapperProvider provider;

    @BeforeAll
    public void setup()
    {
        provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Slice.class, new SliceSerialization.SliceSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(Slice.class, new SliceSerialization.SliceDeserializer()));
    }

    @AfterAll
    public void teardown()
    {
        provider = null;
    }

    @Test
    public void testRoundTrip()
            throws JsonProcessingException
    {
        testRoundTrip(new byte[] {});
        testRoundTrip(new byte[] {1});
        testRoundTrip(new byte[] {1, 2});
        testRoundTrip(new byte[] {1, 2, 3});
        byte[] randomBytes = new byte[1022];
        ThreadLocalRandom.current().nextBytes(randomBytes);
        testRoundTrip(randomBytes);
    }

    private void testRoundTrip(byte[] bytes)
            throws JsonProcessingException
    {
        testRoundTrip(Slices.wrappedBuffer(bytes));

        Slice slice = Slices.wrappedBuffer(new byte[bytes.length + 3], 2, bytes.length);
        slice.setBytes(0, bytes);
        testRoundTrip(slice);

        slice = Slices.allocate(bytes.length);
        slice.setBytes(0, bytes);
        testRoundTrip(slice);
    }

    private void testRoundTrip(Slice slice)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = provider.get();
        Container expected = new Container(slice);
        String json = objectMapper.writeValueAsString(expected);
        Container actual = objectMapper.readValue(json, Container.class);
        assertThat(actual).isEqualTo(expected);
    }

    public record Container(Slice value)
    {
        public Container
        {
            requireNonNull(value, "value is null");
        }
    }
}
