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
import io.varada.cloudvendors.model.StorageObjectMetadata;

import java.io.Serializable;

public record RowGroupDataValidation(
        @JsonProperty("file_modified_time") long fileModifiedTime,
        @JsonProperty("file_content_length") long fileContentLength)
        implements Serializable
{
    @JsonCreator
    public RowGroupDataValidation
    {
    }

    public RowGroupDataValidation(StorageObjectMetadata storageObjectMetadata)
    {
        this(storageObjectMetadata.getLastModified().orElse(0L),
                storageObjectMetadata.getContentLength().orElse(0L));
    }

    public boolean isValid()
    {
        return (fileModifiedTime != 0) && (fileContentLength != 0);
    }
}
