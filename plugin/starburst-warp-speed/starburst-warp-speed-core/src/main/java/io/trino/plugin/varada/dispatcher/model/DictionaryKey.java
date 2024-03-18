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
package io.trino.plugin.varada.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

public record DictionaryKey(
        SchemaTableColumn schemaTableColumn,
        String nodeIdentifier,
        long createdTimestamp)
        implements Serializable
{
    public static final int CREATED_TIMESTAMP_UNKNOWN = -1;

    @Serial private static final long serialVersionUID = 4226795133991077154L;

    @JsonCreator
    public DictionaryKey(
            @JsonProperty("schemaTableColumn") SchemaTableColumn schemaTableColumn,
            @JsonProperty("nodeIdentifier") String nodeIdentifier,
            @JsonProperty("createdTimestamp") long createdTimestamp)
    {
        this.schemaTableColumn = schemaTableColumn;
        this.nodeIdentifier = nodeIdentifier;
        this.createdTimestamp = createdTimestamp;
    }

    public String stringFileNameRepresentation()
    {
        return "%s/%s/%s_%s.dict".formatted(
                schemaTableColumn.schemaTableName().getSchemaName(),
                schemaTableColumn.schemaTableName().getTableName(),
                schemaTableColumn.varadaColumn().getName(),
                nodeIdentifier);
    }
}
