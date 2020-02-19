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
package io.prestosql.parquet.reader;

import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

public class DateColumnReader
        extends PrimitiveColumnReader
{
    @Override
    protected void readValue(BlockBuilder blockBuilder)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT32) {
                sourceType.writeLong(blockBuilder, valuesReader.readInteger());
            }
            else {
                throw new UnsupportedOperationException(format("Could not read type DATE from %s", columnDescriptor.getPrimitiveType()));
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT32) {
                valuesReader.readInteger();
            }
            else {
                throw new UnsupportedOperationException(format("Could not read type DATE from %s", columnDescriptor.getPrimitiveType()));
            }
        }
    }

    public DateColumnReader(RichColumnDescriptor columnDescriptor, Type sourceType, Type targetType)
    {
        super(columnDescriptor, sourceType, targetType);
    }
}
