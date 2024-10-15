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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.server.protocol.spooling.SpooledBlock.SPOOLING_METADATA_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class QueryResultRows
{
    private final Session session;
    private final Optional<List<OutputColumn>> columns;
    private final List<Page> pages;
    private final long totalRows;

    private QueryResultRows(Session session, Optional<List<OutputColumn>> columns, List<Page> pages)
    {
        this.session = requireNonNull(session, "session is null");
        this.columns = requireNonNull(columns, "columns is null").map(values -> values.stream()
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

    public Optional<List<OutputColumn>> getOutputColumns()
    {
        return this.columns;
    }

    public Optional<List<Column>> getColumns()
    {
        return columns
                .map(columns -> columns.stream()
                .map(value -> createColumn(value.columnName(), value.type(), true))
                .collect(toImmutableList()));
    }

    public List<Page> getPages()
    {
        return this.pages;
    }

    /**
     * Returns expected row count (we don't know yet if every row is serializable).
     */
    @VisibleForTesting
    public long getTotalRowsCount()
    {
        return totalRows;
    }

    public Optional<Long> getUpdateCount()
    {
        // We should have exactly single bigint value as an update count.
        if (totalRows != 1 || columns.isEmpty()) {
            return Optional.empty();
        }

        List<OutputColumn> columns = this.columns.get();

        if (columns.size() != 1 || !columns.get(0).type().equals(BIGINT)) {
            return Optional.empty();
        }

        checkState(!pages.isEmpty(), "no data pages available");
        Number value = (Number) columns.get(0).type().getObjectValue(session.toConnectorSession(), pages.getFirst().getBlock(0), 0);

        return Optional.ofNullable(value).map(Number::longValue);
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
                .add("totalRowsCount", getTotalRowsCount())
                .add("pagesCount", this.pages.size())
                .toString();
    }

    public static QueryResultRows empty(Session session)
    {
        return new QueryResultRows(session, Optional.empty(), ImmutableList.of());
    }

    public static Builder queryResultRowsBuilder(Session session)
    {
        return new Builder(session);
    }

    public static class Builder
    {
        private final Session session;
        private ImmutableList.Builder<Page> pages = ImmutableList.builder();
        private Optional<List<OutputColumn>> columns = Optional.empty();

        public Builder(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

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

        public Builder withColumnsAndTypes(@Nullable List<Column> columns, @Nullable List<Type> types)
        {
            if (columns != null || types != null) {
                this.columns = Optional.of(combine(columns, types));
            }

            return this;
        }

        public Builder withSingleBooleanValue(Column column, boolean value)
        {
            BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 1);
            BOOLEAN.writeBoolean(blockBuilder, value);
            pages = ImmutableList.<Page>builder().add(new Page(blockBuilder.build()));
            columns = Optional.of(combine(ImmutableList.of(column), ImmutableList.of(BOOLEAN)));

            return this;
        }

        public QueryResultRows build()
        {
            return new QueryResultRows(
                    session,
                    columns,
                    pages.build());
        }

        private static List<OutputColumn> combine(@Nullable List<Column> columns, @Nullable List<Type> types)
        {
            checkArgument(columns != null && types != null, "columns and types must be present at the same time");
            checkArgument(columns.size() == types.size(), "columns and types size mismatch");

            ImmutableList.Builder<OutputColumn> builder = ImmutableList.builderWithExpectedSize(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                builder.add(new OutputColumn(i, columns.get(i).getName(), types.get(i)));
            }

            return builder.build();
        }
    }
}
