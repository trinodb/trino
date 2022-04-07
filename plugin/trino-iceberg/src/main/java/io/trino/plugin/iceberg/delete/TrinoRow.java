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

import com.google.common.collect.AbstractIterator;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.iceberg.StructLike;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.IcebergPageSink.getIcebergValue;
import static java.util.Objects.requireNonNull;

public class TrinoRow
        implements StructLike
{
    private final Type[] types;
    private final Page page;
    private final int position;

    private TrinoRow(Type[] types, Page page, int position)
    {
        this.types = requireNonNull(types, "types list is null");
        this.page = requireNonNull(page, "page is null");
        checkArgument(position >= 0, "page position must be non-negative: %s", position);
        this.position = position;
    }

    /**
     * Gets the position in the Block this row was originally created from.
     */
    public int getPosition()
    {
        return position;
    }

    @Override
    public int size()
    {
        return page.getChannelCount();
    }

    @Override
    public <T> T get(int i, Class<T> aClass)
    {
        return aClass.cast(getIcebergValue(page.getBlock(i), position, types[i]));
    }

    @Override
    public <T> void set(int i, T t)
    {
        throw new UnsupportedOperationException();
    }

    public static Iterable<TrinoRow> fromPage(Type[] types, Page page, int positionCount)
    {
        return () -> new AbstractIterator<>() {
            private int nextPosition;

            @Nullable
            @Override
            protected TrinoRow computeNext()
            {
                if (nextPosition == positionCount) {
                    return endOfData();
                }
                int position = nextPosition;
                nextPosition++;
                return new TrinoRow(types, page, position);
            }
        };
    }
}
