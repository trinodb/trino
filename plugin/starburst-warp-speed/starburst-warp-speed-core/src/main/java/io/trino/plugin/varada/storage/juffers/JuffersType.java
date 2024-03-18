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
package io.trino.plugin.varada.storage.juffers;

public enum JuffersType
{
    RECORD(0),
    NULL(1),
    LUCENE(2),
    LuceneBMResult(3),
    CRC(4),
    VARLENMD(5),
    EXTENDED_REC(6),
    CHUNKS_MAP(7),
    NUM_TYPES(8);

    private final int varadaBufferTypes;

    JuffersType(int intValue)
    {
        this.varadaBufferTypes = intValue;
    }

    public int value()
    {
        return varadaBufferTypes;
    }
}
