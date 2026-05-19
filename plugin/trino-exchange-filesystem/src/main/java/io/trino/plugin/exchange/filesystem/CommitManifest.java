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
package io.trino.plugin.exchange.filesystem;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class CommitManifest
{
    public static final String FILE_NAME = "committed";

    private static final JsonCodec<CommitManifest> CODEC = jsonCodec(CommitManifest.class);

    private final List<FileStatus> files;

    @JsonCreator
    public CommitManifest(@JsonProperty("files") List<FileStatus> files)
    {
        this.files = requireNonNull(files, "files is null");
    }

    @JsonProperty
    public List<FileStatus> files()
    {
        return files;
    }

    public String serialize()
    {
        return CODEC.toJson(this);
    }

    public static CommitManifest deserialize(byte[] data)
    {
        return CODEC.fromJson(data);
    }
}
