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
import io.trino.spi.connector.ConnectorPartitioningHandle;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class PinotPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final Optional<List<String>> nodes;
    private final Optional<PinotDateTimeField> dateTimeField;
    private final OptionalInt segmentCount;

    @JsonCreator
    public PinotPartitioningHandle(
            @JsonProperty("nodes") Optional<List<String>> nodes,
            @JsonProperty("dateTimeField") Optional<PinotDateTimeField> dateTimeField,
            @JsonProperty("segmentCount") OptionalInt segmentCount)
    {
        this.nodes = requireNonNull(nodes, "nodes is null");
        this.dateTimeField = requireNonNull(dateTimeField, "dateTimeField is null");
        this.segmentCount = requireNonNull(segmentCount, "segmentCount is null");
    }

    @JsonProperty
    public Optional<List<String>> getNodes()
    {
        return nodes;
    }

    @JsonProperty
    public Optional<PinotDateTimeField> getDateTimeField()
    {
        return dateTimeField;
    }

    @JsonProperty
    public OptionalInt getSegmentCount()
    {
        return segmentCount;
    }
}
