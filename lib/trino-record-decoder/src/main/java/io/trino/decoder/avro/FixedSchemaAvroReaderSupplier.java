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
package io.trino.decoder.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import java.nio.ByteBuffer;

public class FixedSchemaAvroReaderSupplier<T>
        implements AvroReaderSupplier<T>
{
    private final DatumReader<T> avroReader;

    public FixedSchemaAvroReaderSupplier(Schema schema)
    {
        avroReader = new GenericDatumReader<>(schema);
    }

    @Override
    public DatumReader<T> get(ByteBuffer data)
    {
        return avroReader;
    }

    public static class Factory
            implements AvroReaderSupplier.Factory
    {
        @Override
        public <T> AvroReaderSupplier<T> create(Schema schema)
        {
            return new FixedSchemaAvroReaderSupplier<>(schema);
        }
    }
}
