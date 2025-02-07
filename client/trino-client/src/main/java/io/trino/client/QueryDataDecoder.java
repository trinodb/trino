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
package io.trino.client;

import io.trino.client.spooling.DataAttributes;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface QueryDataDecoder
{
    interface Factory
    {
        QueryDataDecoder create(List<Column> columns, DataAttributes attributes);

        String encoding();
    }

    /**
     * Decodes the input stream into a lazy ResultRows.
     * <p>
     * Decoder is responsible for closing input stream when
     * all values are decoded or exception was thrown.
     *
     * @param input Input stream to decode (in memory or over the network)
     * @param segmentAttributes Attributes of the segment
     *
     * @throws IOException if an I/O error occurs
     */
    CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
            throws IOException;

    String encoding();
}
