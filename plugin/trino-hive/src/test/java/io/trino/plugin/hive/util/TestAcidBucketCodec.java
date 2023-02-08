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

import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestAcidBucketCodec
{
    @Test
    public void testGetBucketCodecVersion0()
    {
        AcidBucketCodec codec = AcidBucketCodec.forBucket(0);
        assertEquals(codec, AcidBucketCodec.V0);
        assertEquals(codec.decodeWriterId(7), 7);
        assertEquals(codec.decodeStatementId(100), 0);
        assertEquals(codec.decodeStatementId(-10), 0);
    }

    @Test
    public void testGetBucketCodecVersion1()
    {
        AcidBucketCodec codec = AcidBucketCodec.forBucket(1 << 29);
        assertEquals(codec, AcidBucketCodec.V1);
        assertEquals(codec.decodeWriterId(0x0ABC0000), 2748);
        assertEquals(codec.decodeStatementId(0x00000ABC), 2748);
    }

    @Test
    public void testGetBucketCodecInvalidVersion()
    {
        assertThatThrownBy(() -> AcidBucketCodec.forBucket(3 << 29))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid bucket 0x60000000. Version=3");
    }
}
