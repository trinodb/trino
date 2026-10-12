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
package io.trino.filesystem;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.enumeration;

public interface DataStreamProvider
{
    // buffers must be backed by an accessible array
    List<ByteBuffer> takeByteBuffers();

    default InputStream takeInputStream()
    {
        return new SequenceInputStream(enumeration(takeByteBuffers().stream()
                .map(buffer -> new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining()))
                .collect(toImmutableList())));
    }

    int allocated();

    int size();
}
