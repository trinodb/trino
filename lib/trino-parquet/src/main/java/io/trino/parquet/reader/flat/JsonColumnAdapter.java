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

import io.trino.json.JsonBlock;
import io.trino.spi.block.Block;

import java.util.Optional;

public final class JsonColumnAdapter
        extends BinaryColumnAdapter
{
    public static final JsonColumnAdapter JSON_ADAPTER = new JsonColumnAdapter();

    @Override
    protected Block createBlock(BinaryBuffer values, Optional<long[]> valueIsValid)
    {
        return new JsonBlock(values.getValueCount(), valueIsValid, values.getOffsets(), values.asSlice());
    }
}
