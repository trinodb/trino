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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestSliceSerialization
{
    private ObjectMapperProvider provider;

    @BeforeClass
    public void setup()
    {
        provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Slice.class, new SliceSerialization.SliceSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(Slice.class, new SliceSerialization.SliceDeserializer()));
    }

    @AfterClass(alwaysRun = true)
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
        assertEquals(actual, expected);
    }

    public static class Container
    {
        private final Slice value;

        @JsonCreator
        public Container(@JsonProperty("value") Slice value)
        {
            this.value = requireNonNull(value, "value is null");
        }

        @JsonProperty
        public Slice getValue()
        {
            return value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Container container = (Container) o;
            return Objects.equals(value, container.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .toString();
        }
    }
}
