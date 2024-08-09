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
package io.airlift.compress.lz4;

public class Lz4RawCompressor
{
    private static final int HASH_LOG = 12;

    public static final int MAX_TABLE_SIZE = (1 << HASH_LOG);

    private Lz4RawCompressor() {}

    public static int maxCompressedLength(int sourceLength)
    {
        return sourceLength + sourceLength / 255 + 16;
    }
}
