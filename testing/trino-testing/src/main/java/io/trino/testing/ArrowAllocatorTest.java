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
package io.trino.testing;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class ArrowAllocatorTest
{
    private ArrowAllocatorTest() {}

    public static void main(String[] args)
    {
        try {
            System.out.println("Setting up RootAllocator...");

            // Create a new RootAllocator with default config
            BufferAllocator allocator = new RootAllocator();

            System.out.println("RootAllocator created successfully!");

            // Allocate 128 bytes of memory
            try (ArrowBuf buffer = allocator.buffer(128)) {
                System.out.println("Buffer allocated with capacity: " + buffer.capacity());

                // Write a byte
                buffer.setByte(0, 42);
                System.out.println("First byte value: " + buffer.getByte(0));
            }

            System.out.println("Buffer released. Test successful.");

            // Close allocator
            allocator.close();
            System.out.println("Allocator closed.");
        }
        catch (Throwable t) {
            System.err.println("ERROR: Failed to initialize RootAllocator");
            t.printStackTrace();
        }
    }
}
