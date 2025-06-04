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

import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.baseBlockBuildingDecoderFor;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.baseTypeFor;

public class BaseAvroTypeBlockHandler
        implements AvroTypeBlockHandler
{
    @Override
    public void configure(Map<String, byte[]> fileMetadata) {}

    @Override
    public Type typeFor(Schema schema)
            throws AvroTypeException
    {
        return baseTypeFor(schema, this);
    }

    @Override
    public BlockBuildingDecoder blockBuildingDecoderFor(AvroReadAction readAction)
            throws AvroTypeException
    {
        return baseBlockBuildingDecoderFor(readAction, this);
    }
}
