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
package io.trino.hive.formats.line;

import com.google.common.collect.ImmutableList;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

public interface LineDeserializer
{
    LineDeserializer EMPTY_LINE_DESERIALIZER = new LineDeserializer() {
        @Override
        public List<? extends Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public void deserialize(LineBuffer lineBuffer, PageBuilder builder)
        {
            builder.declarePosition();
        }
    };

    /**
     * Required types for the deserialize page builder.
     */
    List<? extends Type> getTypes();

    /**
     * Deserialize the line into the page builder. The implementation will
     * declare the added positions in the page builder.  The implementation
     * is allowed to add zero or more positions to the builder.
     *
     * @param lineBuffer the line which may be empty
     * @param builder page builder for the declared types
     *
     * @throws IOException if line can not be decoded and processing should stop
     * @throws RuntimeException if line can not be decoded and processing should stop
     */
    void deserialize(LineBuffer lineBuffer, PageBuilder builder)
            throws IOException;
}
