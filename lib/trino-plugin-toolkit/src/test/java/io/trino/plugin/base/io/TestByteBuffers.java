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
package io.trino.plugin.base.io;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static io.trino.plugin.base.io.ByteBuffers.getWrappedBytes;
import static org.testng.Assert.assertEquals;

public class TestByteBuffers
{
    @Test
    public void testGetWrappedBytes()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
        assertEquals(getWrappedBytes(buffer), new byte[] {0, 1, 2, 3}, "getWrappedBytes");

        // Assert the buffer position hasn't changed
        assertEquals(buffer.position(), 0, "position");
        assertEquals(getWrappedBytes(buffer), new byte[] {0, 1, 2, 3}, "getWrappedBytes again");
    }
}
