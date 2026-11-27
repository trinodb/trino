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

/// A table argument specification for a table function that needs only the
/// referenced table's [io.trino.spi.connector.ConnectorTableMetadata], not its rows. The
/// engine does not plan a source or read any rows for this argument: it must be passed as
/// a plain `catalog.schema.table` reference (no aliasing, partitioning, ordering, or
/// empty-table treatment), and the referenced table's metadata is passed to the table
/// function as a [TableMetadataArgument].
public class TableMetadataArgumentSpecification
        extends ArgumentSpecification
{
    private TableMetadataArgumentSpecification(String name)
    {
        super(name, true, null);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String name;

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public TableMetadataArgumentSpecification build()
        {
            return new TableMetadataArgumentSpecification(name);
        }
    }
}
