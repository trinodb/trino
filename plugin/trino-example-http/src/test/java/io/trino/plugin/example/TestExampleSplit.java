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
package io.trino.plugin.example;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExampleSplit
{
    private final ExampleSplit split = new ExampleSplit("http://127.0.0.1/test.file");

    @Test
    public void testAddresses()
    {
        // http split with default port
        ExampleSplit httpSplit = new ExampleSplit("http://example.com/example");
        assertThat(httpSplit.getAddresses()).isEqualTo(ImmutableList.of(HostAddress.fromString("example.com")));
        assertThat(httpSplit.isRemotelyAccessible()).isEqualTo(true);

        // http split with custom port
        httpSplit = new ExampleSplit("http://example.com:8080/example");
        assertThat(httpSplit.getAddresses()).isEqualTo(ImmutableList.of(HostAddress.fromParts("example.com", 8080)));
        assertThat(httpSplit.isRemotelyAccessible()).isEqualTo(true);

        // http split with default port
        ExampleSplit httpsSplit = new ExampleSplit("https://example.com/example");
        assertThat(httpsSplit.getAddresses()).isEqualTo(ImmutableList.of(HostAddress.fromString("example.com")));
        assertThat(httpsSplit.isRemotelyAccessible()).isEqualTo(true);

        // http split with custom port
        httpsSplit = new ExampleSplit("https://example.com:8443/example");
        assertThat(httpsSplit.getAddresses()).isEqualTo(ImmutableList.of(HostAddress.fromParts("example.com", 8443)));
        assertThat(httpsSplit.isRemotelyAccessible()).isEqualTo(true);
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<ExampleSplit> codec = jsonCodec(ExampleSplit.class);
        String json = codec.toJson(split);
        ExampleSplit copy = codec.fromJson(json);
        assertThat(copy.getUri()).isEqualTo(split.getUri());

        assertThat(copy.getAddresses()).isEqualTo(ImmutableList.of(HostAddress.fromString("127.0.0.1")));
        assertThat(copy.isRemotelyAccessible()).isEqualTo(true);
    }
}
