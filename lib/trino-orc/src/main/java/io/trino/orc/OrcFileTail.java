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
package io.trino.orc;

import io.airlift.slice.Slice;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.PostScript.HiveWriterVersion;

import static java.util.Objects.requireNonNull;

public record OrcFileTail(
        HiveWriterVersion hiveWriterVersion,
        int bufferSize,
        CompressionKind compressionKind,
        Slice footerSlice,
        int footerSize,
        Slice metadataSlice,
        int metadataSize)
{
    public OrcFileTail {
        requireNonNull(hiveWriterVersion, "hiveWriterVersion is null");
        requireNonNull(compressionKind, "compressionKind is null");
        requireNonNull(footerSlice, "footerSlice is null");
        requireNonNull(metadataSlice, "metadataSlice is null");
    }

    public int getRetainedSize() {
        // Track footer and metadata bytes
        return footerSize + metadataSize;
    }
}
