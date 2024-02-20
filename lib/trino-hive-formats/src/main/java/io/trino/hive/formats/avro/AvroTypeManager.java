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
package io.trino.hive.formats.avro;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public interface AvroTypeManager
{
    /**
     * Called when the type manager is reading out data from a data file such as in {@link AvroFileReader}
     *
     * @param fileMetadata metadata from the file header
     */
    void configure(Map<String, byte[]> fileMetadata);

    Optional<Type> overrideTypeForSchema(Schema schema)
            throws AvroTypeException;

    /**
     * Object provided by FasterReader's deserialization with no conversions.
     * Object class determined by Avro's standard generic data process
     * BlockBuilder provided by Type returned above for the schema
     * Possible to override for each primitive type as well.
     */
    Optional<BiConsumer<BlockBuilder, Object>> overrideBuildingFunctionForSchema(Schema schema)
            throws AvroTypeException;

    /**
     * Extract and convert the object from the given block at the given position and return the Avro Generic Data forum.
     * Type is either provided explicitly to the writer or derived from the schema using this interface.
     */
    Optional<BiFunction<Block, Integer, Object>> overrideBlockToAvroObject(Schema schema, Type type)
            throws AvroTypeException;
}
