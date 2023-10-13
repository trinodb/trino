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
package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLocalFileSplit
{
    private final HostAddress address = HostAddress.fromParts("localhost", 1234);
    private final LocalFileSplit split = new LocalFileSplit(address);

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<LocalFileSplit> codec = jsonCodec(LocalFileSplit.class);
        String json = codec.toJson(split);
        LocalFileSplit copy = codec.fromJson(json);

        assertThat(copy.getAddress()).isEqualTo(split.getAddress());

        assertThat(copy.getAddresses()).isEqualTo(ImmutableList.of(address));
        assertThat(copy.isRemotelyAccessible()).isEqualTo(false);
    }
}
