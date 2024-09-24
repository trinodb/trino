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
package io.trino.plugin.pulsar.decoder;

import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.block.Block;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Map;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Abstract util superclass for  XXDecoderTestUtil (e.g. AvroDecoderTestUtil 、JsonDecoderTestUtil)
 */
public abstract class DecoderTestUtil
{
    protected static final CatalogName catalogName = new CatalogName("test-connector");

    protected DecoderTestUtil() {}

    public abstract void checkArrayValues(Block block, Type type, Object value);

    public abstract void checkMapValues(Block block, Type type, Object value);

    public abstract void checkRowValues(Block block, Type type, Object value);

    public abstract void checkPrimitiveValue(Object actual, Object expected);

    public static CatalogName getCatalogName()
    {
        assertNotNull(catalogName);
        return catalogName;
    }

    public void checkField(Block actualBlock, Type type, int position, Object expectedValue)
    {
        assertNotNull(type, "Type is null");
        assertNotNull(actualBlock, "actualBlock is null");
        assertTrue(!actualBlock.isNull(position));
        assertNotNull(expectedValue, "expectedValue is null");

        if (type instanceof ArrayType) {
            checkArrayValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        }
        else if (type instanceof MapType) {
            checkMapValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        }
        else if (type instanceof RowType) {
            checkRowValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        }
        else {
            checkPrimitiveValue(getObjectValue(type, actualBlock, position), expectedValue);
        }
    }

    public boolean isIntegralType(Object value)
    {
        return value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte;
    }

    public boolean isRealType(Object value)
    {
        return value instanceof Float || value instanceof Double;
    }

    public Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return type.getObjectValue(SESSION, block, position);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, Slice value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getSlice(), value);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, String value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getSlice().toStringUtf8(), value);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getLong(), value);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, double value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getDouble(), value, 0.0001);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, boolean value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getBoolean(), value);
    }

    public void checkIsNull(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertTrue(provider.isNull());
    }
}
