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

import io.airlift.slice.SliceOutput;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

public interface LineSerializer
{
    /**
     * Required types for the deserialize page builder.
     */
    List<? extends Type> getTypes();

    /**
     * Serialize the page position into the page builder.
     *
     * @throws IOException if line can not be written and processing should stop
     * @throws RuntimeException if line can not be decoded and processing should stop
     */
    void write(Page page, int position, SliceOutput sliceOutput)
            throws IOException;
}
