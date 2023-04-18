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

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.StructLike;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public final class EqualityDeleteFilter
        implements DeleteFilter
{
    private final Predicate<StructLike> deletedRows;

    public EqualityDeleteFilter(Predicate<StructLike> deletedRows)
    {
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        return (page, position) -> {
            StructLike row = new LazyTrinoRow(types, page, position);
            return !deletedRows.test(row);
        };
    }

    public static void readEqualityDeletes(ConnectorPageSource pageSource, List<IcebergColumnHandle> columns, Consumer<StructLike> deletedRows)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                deletedRows.accept(new TrinoRow(types, page, position));
            }
        }
    }
}
