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
package io.trino.spi.function.table;

import static io.trino.spi.function.table.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableArgumentSpecification
        extends ArgumentSpecification
{
    private final boolean rowSemantics;
    private final boolean pruneWhenEmpty;
    private final boolean passThroughColumns;
    private final boolean useTableMetadata;

    private TableArgumentSpecification(String name, boolean rowSemantics, Boolean pruneWhenEmpty, boolean passThroughColumns, boolean useTableMetadata)
    {
        super(name, true, null);

        requireNonNull(pruneWhenEmpty, "The pruneWhenEmpty property is not set");
        checkArgument(!rowSemantics || pruneWhenEmpty, "Cannot set the KEEP WHEN EMPTY property for a table argument with row semantics");
        checkArgument(!useTableMetadata || !rowSemantics, "Cannot set the row semantics property for a table argument using table metadata");
        checkArgument(!useTableMetadata || !passThroughColumns, "Cannot set the pass-through columns property for a table argument using table metadata");

        this.rowSemantics = rowSemantics;
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughColumns = passThroughColumns;
        this.useTableMetadata = useTableMetadata;
    }

    public boolean isRowSemantics()
    {
        return rowSemantics;
    }

    public boolean isPruneWhenEmpty()
    {
        return pruneWhenEmpty;
    }

    public boolean isPassThroughColumns()
    {
        return passThroughColumns;
    }

    /**
     * When {@code true}, the engine does not plan a source or read any rows for this
     * argument. Instead, the argument must be passed as a plain {@code catalog.schema.table}
     * reference (no aliasing, partitioning, ordering, or empty-table treatment), and the
     * referenced table's {@link io.trino.spi.connector.ConnectorTableMetadata} is passed to
     * the table function as a {@link TableMetadataArgument}.
     */
    public boolean isUseTableMetadata()
    {
        return useTableMetadata;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String name;
        private boolean rowSemantics;
        private Boolean pruneWhenEmpty;
        private boolean passThroughColumns;
        private boolean useTableMetadata;

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder rowSemantics()
        {
            this.rowSemantics = true;
            this.pruneWhenEmpty = true;
            return this;
        }

        public Builder pruneWhenEmpty()
        {
            this.pruneWhenEmpty = true;
            return this;
        }

        public Builder keepWhenEmpty()
        {
            this.pruneWhenEmpty = false;
            return this;
        }

        public Builder passThroughColumns()
        {
            this.passThroughColumns = true;
            return this;
        }

        public Builder useTableMetadata()
        {
            this.useTableMetadata = true;
            this.pruneWhenEmpty = true;
            return this;
        }

        public TableArgumentSpecification build()
        {
            return new TableArgumentSpecification(name, rowSemantics, pruneWhenEmpty, passThroughColumns, useTableMetadata);
        }
    }
}
