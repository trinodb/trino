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
package io.trino.spi.connector;

import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class TableScanRedirectApplicationResult
{
    private final List<Redirection> redirections;

    public TableScanRedirectApplicationResult(CatalogSchemaTableName destinationTable, Map<ColumnHandle, String> destinationColumns, TupleDomain<String> filter)
    {
        this.redirections = singletonList(new Redirection(destinationTable, destinationColumns, filter));
    }

    /**
     * Creates a UNION ALL redirection that is composed on individual table scans with given predicates
     */
    public TableScanRedirectApplicationResult(List<Redirection> redirections)
    {
        if (redirections.isEmpty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Empty redirections list");
        }
        this.redirections = List.copyOf(requireNonNull(redirections, "redirections is null"));
    }

    public List<Redirection> getRedirections()
    {
        return redirections;
    }

    public static class Redirection
    {
        private final CatalogSchemaTableName destinationTable;
        // mapping of source table handles to destination table columns
        private final Map<ColumnHandle, String> destinationColumns;
        // filter that needs to be applied on top of table scan
        private final TupleDomain<String> filter;

        public Redirection(CatalogSchemaTableName destinationTable, Map<ColumnHandle, String> destinationColumns, TupleDomain<String> filter)
        {
            this.destinationTable = requireNonNull(destinationTable, "destinationTable is null");
            this.destinationColumns = Map.copyOf(requireNonNull(destinationColumns, "destinationColumns is null"));
            this.filter = requireNonNull(filter, "filter is null");
        }

        public CatalogSchemaTableName getDestinationTable()
        {
            return destinationTable;
        }

        public Map<ColumnHandle, String> getDestinationColumns()
        {
            return destinationColumns;
        }

        public TupleDomain<String> getFilter()
        {
            return filter;
        }
    }
}
