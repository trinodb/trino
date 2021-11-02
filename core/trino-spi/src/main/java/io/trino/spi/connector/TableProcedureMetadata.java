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

import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Objects.requireNonNull;

public class TableProcedureMetadata
{
    // Name must be uppercase if procedure is to be executed without delimitation via ALTER TABLE ... EXECUTE syntax
    private final String name;
    private final TableProcedureExecutionMode executionMode;
    private final List<PropertyMetadata<?>> properties;

    public TableProcedureMetadata(String name, TableProcedureExecutionMode executionMode, List<PropertyMetadata<?>> properties)
    {
        this.name = checkNotEmpty(name, "name");
        this.executionMode = requireNonNull(executionMode, "executionMode is null");
        this.properties = List.copyOf(requireNonNull(properties, "properties is null"));
    }

    public String getName()
    {
        return name;
    }

    public TableProcedureExecutionMode getExecutionMode()
    {
        return executionMode;
    }

    public List<PropertyMetadata<?>> getProperties()
    {
        return properties;
    }
}
