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
package io.trino.plugin.jmx;

import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.jmx.MetadataUtil.SPLIT_CODEC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJmxSplit
{
    private static final ImmutableList<HostAddress> ADDRESSES = ImmutableList.of(HostAddress.fromString("test:1234"));
    private static final JmxSplit SPLIT = new JmxSplit(ADDRESSES);

    @Test
    public void testSplit()
    {
        assertThat(SPLIT.getAddresses()).isEqualTo(ADDRESSES);
        assertThat(SPLIT.getInfo()).isSameAs(SPLIT);
        assertThat(SPLIT.isRemotelyAccessible()).isFalse();
    }

    @Test
    public void testJsonRoundTrip()
    {
        String json = SPLIT_CODEC.toJson(SPLIT);
        JmxSplit copy = SPLIT_CODEC.fromJson(json);

        assertThat(copy.getAddresses()).isEqualTo(SPLIT.getAddresses());
        assertThat(copy.getInfo()).isSameAs(copy);
        assertThat(copy.isRemotelyAccessible()).isFalse();
    }
}
