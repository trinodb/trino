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
package io.trino.plugin.iceberg.delete;

import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.iceberg.StructLike;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.IcebergPageSink.getIcebergValue;

final class TrinoRow
        implements StructLike
{
    private final Object[] values;

    public TrinoRow(Type[] types, Page page, int position)
    {
        checkArgument(types.length == page.getChannelCount(), "mismatched types for page");
        values = new Object[types.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = getIcebergValue(page.getBlock(i), position, types[i]);
        }
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public <T> T get(int i, Class<T> clazz)
    {
        return clazz.cast(values[i]);
    }

    @Override
    public <T> void set(int i, T t)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "TrinoRow" + Arrays.toString(values);
    }
}
