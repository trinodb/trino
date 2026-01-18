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
package io.trino.plugin.iceberg;

/**
 * Defines write modes for delete, update, and merge operations in Iceberg tables.
 * <p>
 * <b>MERGE_ON_READ (MoR)</b>: Changes are recorded in delete files and merged during read operations.
 * <ul>
 *   <li>Pros: Fast writes, lower write amplification, better for frequent small updates</li>
 *   <li>Cons: Slower reads (must merge delete files), more small files to manage</li>
 *   <li>Best for: Write-heavy workloads, frequent updates to small portions of data</li>
 * </ul>
 * <p>
 * <b>COPY_ON_WRITE (CoW)</b>: Data files are completely rewritten when changes are made.
 * <ul>
 *   <li>Pros: Fast reads (no merge needed), cleaner file structure</li>
 *   <li>Cons: Slower writes, high write amplification, temporary storage overhead</li>
 *   <li>Best for: Read-heavy workloads, batch updates affecting large portions of files</li>
 * </ul>
 * <p>
 * The mode can be configured independently for DELETE, UPDATE, and MERGE operations
 * using the table properties: write_delete_mode, write_update_mode, write_merge_mode
 */
public enum UpdateMode
{
    MERGE_ON_READ("merge-on-read"),
    COPY_ON_WRITE("copy-on-write");

    private final String icebergProperty;

    UpdateMode(String icebergProperty)
    {
        this.icebergProperty = icebergProperty;
    }

    /**
     * Returns the Iceberg property value corresponding to this mode
     */
    public String getIcebergProperty()
    {
        return icebergProperty;
    }

    /**
     * Returns the UpdateMode enum value for the given Iceberg property value
     */
    public static UpdateMode fromIcebergProperty(String property)
    {
        for (UpdateMode mode : values()) {
            if (mode.getIcebergProperty().equalsIgnoreCase(property)) {
                return mode;
            }
        }
        return MERGE_ON_READ; // Default for backward compatibility
    }
}
