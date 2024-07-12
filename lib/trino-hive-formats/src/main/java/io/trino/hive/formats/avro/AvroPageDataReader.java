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

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.AvroTypeUtils.verifyNoCircularReferences;
import static java.util.Objects.requireNonNull;

public class AvroPageDataReader
        implements DatumReader<Optional<Page>>
{
    private final Schema readerSchema;
    private Schema writerSchema;
    private final PageBuilder pageBuilder;
    private RowBlockBuildingDecoder rowBlockBuildingDecoder;
    private final AvroTypeBlockHandler typeManager;

    public AvroPageDataReader(Schema readerSchema, AvroTypeBlockHandler typeManager)
            throws AvroTypeException
    {
        this.readerSchema = requireNonNull(readerSchema, "readerSchema is null");
        writerSchema = this.readerSchema;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        verifyNoCircularReferences(readerSchema);
        try {
            Type readerSchemaType = typeManager.typeFor(readerSchema);
            verify(readerSchemaType instanceof RowType, "Root Avro type must be a row");
            pageBuilder = new PageBuilder(readerSchemaType.getTypeParameters());
            initialize();
        }
        catch (org.apache.avro.AvroTypeException e) {
            throw new AvroTypeException(e);
        }
    }

    private void initialize()
            throws AvroTypeException
    {
        verify(readerSchema.getType() == Schema.Type.RECORD, "Avro schema for page reader must be record");
        verify(writerSchema.getType() == Schema.Type.RECORD, "File Avro schema for page reader must be record");
        rowBlockBuildingDecoder = new RowBlockBuildingDecoder(writerSchema, readerSchema, typeManager);
    }

    @Override
    public void setSchema(Schema schema)
    {
        requireNonNull(schema, "schema is null");
        if (schema != writerSchema) {
            writerSchema = schema;
            try {
                initialize();
            }
            catch (org.apache.avro.AvroTypeException e) {
                throw new UncheckedAvroTypeException(new AvroTypeException(e));
            }
            catch (AvroTypeException e) {
                throw new UncheckedAvroTypeException(e);
            }
        }
    }

    @Override
    public Optional<Page> read(Optional<Page> ignoredReuse, Decoder decoder)
            throws IOException
    {
        Optional<Page> page = Optional.empty();
        rowBlockBuildingDecoder.decodeIntoPageBuilder(decoder, pageBuilder);
        if (pageBuilder.isFull()) {
            page = Optional.of(pageBuilder.build());
            pageBuilder.reset();
        }
        return page;
    }

    public Optional<Page> flush()
    {
        if (!pageBuilder.isEmpty()) {
            Optional<Page> lastPage = Optional.of(pageBuilder.build());
            pageBuilder.reset();
            return lastPage;
        }
        return Optional.empty();
    }

    /**
     * Used for throwing {@link AvroTypeException} through interfaces that can not throw checked exceptions like DatumReader
     */
    protected static class UncheckedAvroTypeException
            extends RuntimeException
    {
        private final AvroTypeException avroTypeException;

        public UncheckedAvroTypeException(AvroTypeException cause)
        {
            super(requireNonNull(cause, "cause is null"));
            avroTypeException = cause;
        }

        public AvroTypeException getAvroTypeException()
        {
            return avroTypeException;
        }
    }
}
