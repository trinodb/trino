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

    private TableArgumentSpecification(String name, boolean rowSemantics, Boolean pruneWhenEmpty, boolean passThroughColumns)
    {
        super(name, true, null);

        requireNonNull(pruneWhenEmpty, "The pruneWhenEmpty property is not set");
        checkArgument(!rowSemantics || pruneWhenEmpty, "Cannot set the KEEP WHEN EMPTY property for a table argument with row semantics");

        this.rowSemantics = rowSemantics;
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughColumns = passThroughColumns;
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

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        /**
         * The table argument has row semantics: the table function processes each input row independently.
         * Otherwise it has set semantics, and the table function processes each partition as a whole.
         * A table argument with row semantics cannot be partitioned or ordered, and it is always pruned when empty.
         */
        public Builder rowSemantics()
        {
            this.rowSemantics = true;
            this.pruneWhenEmpty = true;
            return this;
        }

        /**
         * The table function produces no output when this table is empty.
         * This is a default, which a query can override with {@code KEEP WHEN EMPTY}.
         * Either this or {@link #keepWhenEmpty()} must be set for a table argument with set semantics.
         */
        public Builder pruneWhenEmpty()
        {
            this.pruneWhenEmpty = true;
            return this;
        }

        /**
         * The table function is invoked even when this table is empty.
         * This is a default, which a query can override with {@code PRUNE WHEN EMPTY}.
         * It cannot be set for a table argument with row semantics.
         */
        public Builder keepWhenEmpty()
        {
            this.pruneWhenEmpty = false;
            return this;
        }

        /**
         * All columns of this table can be passed through to the output, in addition to the proper columns
         * produced by the table function. For each output row, the table function returns the index of
         * the input row to attach, see {@link TableFunctionProcessorState}.
         */
        public Builder passThroughColumns()
        {
            this.passThroughColumns = true;
            return this;
        }

        public TableArgumentSpecification build()
        {
            return new TableArgumentSpecification(name, rowSemantics, pruneWhenEmpty, passThroughColumns);
        }
    }
}
