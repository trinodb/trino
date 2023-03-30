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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.sql.planner.PartitioningHandle;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TableLayout
{
    private final CatalogHandle catalogHandle;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorTableLayout layout;

    @JsonCreator
    public TableLayout(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle,
            @JsonProperty("layout") ConnectorTableLayout layout)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.layout = requireNonNull(layout, "layout is null");
    }

    @JsonProperty
    public CatalogHandle getCatalogName()
    {
        return catalogHandle;
    }

    @JsonProperty
    public ConnectorTableLayout getLayout()
    {
        return layout;
    }

    public Optional<PartitioningHandle> getPartitioning()
    {
        return layout.getPartitioning()
                .map(partitioning -> new PartitioningHandle(Optional.of(catalogHandle), Optional.of(transactionHandle), partitioning));
    }

    public List<String> getPartitionColumns()
    {
        return layout.getPartitionColumns();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableLayout that = (TableLayout) o;
        return Objects.equals(catalogHandle, that.catalogHandle) &&
                Objects.equals(transactionHandle, that.transactionHandle) &&
                Objects.equals(layout, that.layout);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogHandle, transactionHandle, layout);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogHandle", catalogHandle)
                .add("transactionHandle", transactionHandle)
                .add("layout", layout)
                .toString();
    }
}
