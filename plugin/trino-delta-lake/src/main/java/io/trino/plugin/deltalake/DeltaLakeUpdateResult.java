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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeltaLakeUpdateResult
{
    private final String oldFile;
    private final Optional<DataFileInfo> newFile;

    @JsonCreator
    public DeltaLakeUpdateResult(
            @JsonProperty("oldFile") String oldFile,
            @JsonProperty("newFile") Optional<DataFileInfo> newFile)
    {
        this.oldFile = requireNonNull(oldFile, "oldFile is null");
        this.newFile = requireNonNull(newFile, "newFile is null");
    }

    @JsonProperty
    public String getOldFile()
    {
        return oldFile;
    }

    @JsonProperty
    public Optional<DataFileInfo> getNewFile()
    {
        return newFile;
    }
}
