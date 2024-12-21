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
package io.trino.plugin.iceberg.procedure;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IcebergDropExtendedStatsHandle.class, name = "drop_extended_stats"),
        @JsonSubTypes.Type(value = IcebergExpireSnapshotsHandle.class, name = "expire_snapshots"),
        @JsonSubTypes.Type(value = IcebergOptimizeHandle.class, name = "optimize"),
        @JsonSubTypes.Type(value = IcebergRemoveOrphanFilesHandle.class, name = "remove_orphan_files"),
        @JsonSubTypes.Type(value = IcebergAddFilesHandle.class, name = "add_files"),
        @JsonSubTypes.Type(value = IcebergAddFilesFromTableHandle.class, name = "add_files_from_table"),
        @JsonSubTypes.Type(value = IcebergCreateBranchHandle.class, name = "create_branch"),
        @JsonSubTypes.Type(value = IcebergDropBranchHandle.class, name = "drop_branch"),
        @JsonSubTypes.Type(value = IcebergFastForwardHandle.class, name = "fast_forward"),
})
public interface IcebergProcedureHandle {}
