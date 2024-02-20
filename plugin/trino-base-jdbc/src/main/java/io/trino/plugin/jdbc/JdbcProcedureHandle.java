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
package io.trino.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcProcedureHandle
        extends BaseJdbcConnectorTableHandle
{
    private final ProcedureQuery procedure;
    private final List<JdbcColumnHandle> columns;

    @JsonCreator
    public JdbcProcedureHandle(@JsonProperty ProcedureQuery procedureQuery, @JsonProperty List<JdbcColumnHandle> columns)
    {
        this.procedure = requireNonNull(procedureQuery, "procedureQuery is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    @JsonProperty
    public ProcedureQuery getProcedureQuery()
    {
        return procedure;
    }

    @Override
    @JsonProperty
    public Optional<List<JdbcColumnHandle>> getColumns()
    {
        return Optional.of(columns);
    }

    @Override
    public String toString()
    {
        return format("Procedure[%s], Columns=%s", procedure, columns);
    }

    public record ProcedureQuery(@JsonProperty String query)
    {
        @JsonCreator
        public ProcedureQuery
        {
            requireNonNull(query, "query is null");
        }
    }
}
