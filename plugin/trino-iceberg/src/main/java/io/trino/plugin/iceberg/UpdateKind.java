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
 * Defines types of update operations performed on Iceberg tables.
 * <p>
 * This enum is used to track which type of operation is being performed
 * so that the appropriate write mode (MERGE_ON_READ or COPY_ON_WRITE)
 * can be selected based on the table properties.
 * <p>
 * <ul>
 *   <li><b>DELETE</b>: Row deletion operation - controlled by write_delete_mode property</li>
 *   <li><b>UPDATE</b>: Row update operation - controlled by write_update_mode property</li>
 *   <li><b>MERGE</b>: Merge operation (UPDATE + INSERT) - controlled by write_merge_mode property</li>
 * </ul>
 */
public enum UpdateKind
{
    DELETE,
    UPDATE,
    MERGE
}
