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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final String pinotTableName;
    private final Optional<PinotDateTimeField> dateTimeField;
    private final List<PinotColumnHandle> columnHandles;

    @JsonCreator
    public PinotInsertTableHandle(
            @JsonProperty String pinotTableName,
            @JsonProperty Optional<PinotDateTimeField> dateTimeField,
            @JsonProperty List<PinotColumnHandle> columnHandles)
    {
        this.pinotTableName = requireNonNull(pinotTableName, "pinotTableName is null");
        this.dateTimeField = requireNonNull(dateTimeField, "dateTimeField is null");
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
    }

    @JsonProperty
    public String getPinotTableName()
    {
        return pinotTableName;
    }

    @JsonProperty
    public List<PinotColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public Optional<PinotDateTimeField> getDateTimeField()
    {
        return dateTimeField;
    }
}
