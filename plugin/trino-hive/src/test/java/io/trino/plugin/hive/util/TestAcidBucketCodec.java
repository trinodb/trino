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
package io.trino.plugin.hive.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAcidBucketCodec
{
    @Test
    public void testGetBucketCodecVersion0()
    {
        AcidBucketCodec codec = AcidBucketCodec.forBucket(0);
        assertThat(codec).isEqualTo(AcidBucketCodec.V0);
        assertThat(codec.decodeWriterId(7)).isEqualTo(7);
        assertThat(codec.decodeStatementId(100)).isEqualTo(0);
        assertThat(codec.decodeStatementId(-10)).isEqualTo(0);
    }

    @Test
    public void testGetBucketCodecVersion1()
    {
        AcidBucketCodec codec = AcidBucketCodec.forBucket(1 << 29);
        assertThat(codec).isEqualTo(AcidBucketCodec.V1);
        assertThat(codec.decodeWriterId(0x0ABC0000)).isEqualTo(2748);
        assertThat(codec.decodeStatementId(0x00000ABC)).isEqualTo(2748);
    }

    @Test
    public void testGetBucketCodecInvalidVersion()
    {
        assertThatThrownBy(() -> AcidBucketCodec.forBucket(3 << 29))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid bucket 0x60000000. Version=3");
    }
}
