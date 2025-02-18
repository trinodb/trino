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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import io.trino.client.Column;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.server.protocol.spooling.SpooledBlock.SPOOLING_METADATA_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class QueryResultRows
{
    private final Optional<List<OutputColumn>> columns;
    private final List<Page> pages;
    private final long totalRows;

    private QueryResultRows(Optional<List<OutputColumn>> columns, List<Page> pages)
    {
        this.columns = requireNonNull(columns, "columns is null")
                .map(values -> values.stream()
                .filter(column -> !isSpooledMetadataColumn(column))
                .collect(toImmutableList()));
        this.pages = ImmutableList.copyOf(pages);
        this.totalRows = countRows(pages);

        verify(totalRows == 0 || (totalRows > 0 && columns.isPresent()), "data present without columns and types");
    }

    private boolean isSpooledMetadataColumn(OutputColumn column)
    {
        return column.type().equals(SPOOLING_METADATA_TYPE);
    }

    public boolean isEmpty()
    {
        return totalRows == 0;
    }

    public List<OutputColumn> getOutputColumns()
    {
        return columns.orElseThrow(() -> new IllegalStateException("Columns are not present"));
    }

    public List<Column> getOptionalColumns()
    {
        return columns
                .map(columns -> columns.stream()
                    .map(value -> createColumn(value.columnName(), value.type(), true))
                    .collect(toImmutableList()))
                .orElse(null);
    }

    public List<Page> getPages()
    {
        return this.pages;
    }

    public OptionalLong getUpdateCount()
    {
        // We should have exactly single bigint value as an update count.
        if (totalRows != 1 || columns.isEmpty()) {
            return OptionalLong.empty();
        }

        List<OutputColumn> onlyColumn = columns.get();
        if (onlyColumn.size() != 1 || !onlyColumn.getFirst().type().equals(BIGINT)) {
            return OptionalLong.empty();
        }

        checkState(!pages.isEmpty(), "no data pages available");
        Block block = pages.getFirst().getBlock(0);
        if (block.isNull(0)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(BIGINT.getLong(block, 0));
    }

    private static long countRows(List<Page> pages)
    {
        long rows = 0;
        for (Page page : pages) {
            rows += page.getPositionCount();
        }
        return rows;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .add("totalRowsCount", totalRows)
                .add("pagesCount", pages.size())
                .toString();
    }

    public static QueryResultRows empty()
    {
        return new QueryResultRows(Optional.empty(), ImmutableList.of());
    }

    public static Builder queryResultRowsBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ImmutableList.Builder<Page> pages = ImmutableList.builder();
        private Optional<List<OutputColumn>> columns = Optional.empty();

        public Builder addPage(Page page)
        {
            pages.add(page);
            return this;
        }

        public Builder addPages(List<Page> page)
        {
            pages.addAll(page);
            return this;
        }

        public Builder withColumnsAndTypes(List<Column> columns, List<Type> types)
        {
            this.columns = combine(columns, types);
            return this;
        }

        public QueryResultRows build()
        {
            return new QueryResultRows(columns, pages.build());
        }

        private static Optional<List<OutputColumn>> combine(List<Column> columns, List<Type> types)
        {
            if (columns == null && types == null) {
                return Optional.empty();
            }
            checkArgument(columns != null && types != null, "columns and types must be present at the same time");
            checkArgument(columns.size() == types.size(), "columns and types size mismatch");

            ImmutableList.Builder<OutputColumn> builder = ImmutableList.builderWithExpectedSize(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                builder.add(new OutputColumn(i, columns.get(i).getName(), types.get(i)));
            }

            return Optional.of(builder.build());
        }
    }
}
