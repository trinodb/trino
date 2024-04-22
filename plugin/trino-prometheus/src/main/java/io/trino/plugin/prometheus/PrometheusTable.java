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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public record PrometheusTable(String name, List<PrometheusColumn> columns)
{
    public PrometheusTable
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public List<ColumnMetadata> columnsMetadata()
    {
        return columns.stream()
                .map(column -> new ColumnMetadata(column.name(), column.type()))
                .collect(toImmutableList());
    }
}
