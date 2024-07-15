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

import io.trino.hive.formats.avro.model.AvroReadAction;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.util.Map;

public interface AvroTypeBlockHandler
{
    /**
     * Called when the type manager is reading out data from a data file such as in {@link AvroFileReader}
     *
     * This usage implies interior immutability of the block handler.
     * The avro library (and our usage of it) requires the block handler to be defined when opening up a file.
     * However, Hive uses the metadata from the file to define behavior of the block handler.
     * Instead of writing our own version of the Avro file reader, interior mutability is allowed.
     * This will be called once before any data of the file is read, but after the Trino types are defined for the read schema.
     * It will be called once per file.
     *
     * @param fileMetadata metadata from the file header
     */
    void configure(Map<String, byte[]> fileMetadata);

    /**
     * Returns the block type to build for this Avro schema.
     *
     * @throws AvroTypeException in case there is no Type for or misconfiguration with the given schema
     */
    Type typeFor(Schema schema)
            throws AvroTypeException;

    BlockBuildingDecoder blockBuildingDecoderFor(AvroReadAction readAction)
            throws AvroTypeException;
}
