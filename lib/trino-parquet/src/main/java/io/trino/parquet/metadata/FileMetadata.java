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
package io.trino.parquet.metadata;

import org.apache.parquet.schema.MessageType;

import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class FileMetadata
{
    private final MessageType schema;
    private final Map<String, String> keyValueMetaData;
    private final String createdBy;

    public FileMetadata(MessageType schema, Map<String, String> keyValueMetaData, String createdBy)
    {
        this.schema = requireNonNull(schema, "schema cannot be null");
        this.keyValueMetaData = unmodifiableMap(requireNonNull(keyValueMetaData, "keyValueMetaData cannot be null"));
        this.createdBy = createdBy;
    }

    public MessageType getSchema()
    {
        return schema;
    }

    @Override
    public String toString()
    {
        return "FileMetaData{schema: " + schema + ", metadata: " + keyValueMetaData + "}";
    }

    public Map<String, String> getKeyValueMetaData()
    {
        return keyValueMetaData;
    }

    public String getCreatedBy()
    {
        return createdBy;
    }
}
