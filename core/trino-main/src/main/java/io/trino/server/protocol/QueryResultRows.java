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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;

public class QueryResultRows
{
    private final List<Page> pages;
    private final long totalRows;
    private final List<Type> types;

    private QueryResultRows(List<Type> types, List<Page> pages)
    {
        this.types = types;
        this.pages = ImmutableList.copyOf(pages);
        this.totalRows = countRows(pages);

        verify(totalRows == 0 || (totalRows > 0 && types != null), "data present without types");
    }

    public boolean isEmpty()
    {
        return totalRows == 0;
    }

    public List<Page> getPages()
    {
        return this.pages;
    }

    public OptionalLong getUpdateCount()
    {
        // We should have exactly single bigint value as an update count.
        if (totalRows != 1 || types == null) {
            return OptionalLong.empty();
        }

        if (types.size() != 1 || !types.getFirst().equals(BIGINT)) {
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
                .add("types", types)
                .add("totalRowsCount", totalRows)
                .add("pagesCount", pages.size())
                .toString();
    }

    public static QueryResultRows empty()
    {
        return new QueryResultRows(null, ImmutableList.of());
    }

    public static Builder queryResultRowsBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableList.Builder<Page> pages = ImmutableList.builder();
        private List<Type> types;

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

        public Builder withTypes(List<Type> types)
        {
            this.types = types;
            return this;
        }

        public QueryResultRows build()
        {
            return new QueryResultRows(types, pages.build());
        }
    }
}
