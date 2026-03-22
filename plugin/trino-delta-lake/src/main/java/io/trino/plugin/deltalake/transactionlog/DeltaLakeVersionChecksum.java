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
package io.trino.plugin.deltalake.transactionlog;

import jakarta.annotation.Nullable;

// Ref. https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file
// At this time, we support all fields of the version checksum file that are required per the Delta spec. However, we treat
// all fields as optional to be defensive against non-compliant Delta Lake implementations
public record DeltaLakeVersionChecksum(
        @Nullable Long tableSizeBytes,
        @Nullable Long numFiles,
        @Nullable Long numMetadata,
        @Nullable Long numProtocol,
        @Nullable MetadataEntry metadata,
        @Nullable ProtocolEntry protocol)
{
}
