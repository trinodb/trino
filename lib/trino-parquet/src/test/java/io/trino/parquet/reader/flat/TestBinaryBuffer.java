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
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertSame;

public class TestBinaryBuffer
{
    @Test
    public void testAsSlice()
    {
        BinaryBuffer buffer = new BinaryBuffer(2);
        Slice a = Slices.wrappedBuffer((byte) 0);
        Slice b = Slices.wrappedBuffer((byte) 1);

        buffer.add(a, 0);
        Slice result = buffer.asSlice();
        assertSame(a, result);

        buffer.add(b, 1);

        result = buffer.asSlice();
        Slice secondInvocation = buffer.asSlice();

        assertSame(secondInvocation, result);
        assertThat(result.getBytes()).isEqualTo(new byte[] {0, 1});
    }
}
