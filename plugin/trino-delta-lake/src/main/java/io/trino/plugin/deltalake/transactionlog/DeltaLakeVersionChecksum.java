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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

// Ref. https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file
// Only the fields currently read by Trino are modeled. The spec marks both as required, but
// non-compliant writers may omit them; absent entries trigger fallback to the transaction log.
public record DeltaLakeVersionChecksum(Optional<MetadataEntry> metadata, Optional<ProtocolEntry> protocol)
{
    public DeltaLakeVersionChecksum
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(protocol, "protocol is null");
    }
}
