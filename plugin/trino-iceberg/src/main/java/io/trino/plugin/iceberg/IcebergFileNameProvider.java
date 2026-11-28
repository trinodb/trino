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

import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;

public interface IcebergFileNameProvider
{
    /**
     * Returns the name of a new data or delete file the connector is about to write. This is the
     * file name only, not its path; where the file goes in storage is decided separately. The name
     * must not contain path separators, and must be unique across all writers for the lifetime of
     * the table.
     * A deletion vector is requested as {@link FileContent#POSITION_DELETES} in {@link FileFormat#PUFFIN}.
     */
    String newFileName(ConnectorSession session, FileContent content, FileFormat format);

    /**
     * Returns the name prefix of the temporary files a sorted write spills to before it writes the
     * sorted data file. The connector chooses the directory, and names each temporary file by
     * appending a period and a sequence number to the prefix. The prefix must not contain path
     * separators, and must be unique across all writers of all tables, because sorted writes to
     * different tables can share a local staging directory.
     */
    String newSortTempFilePrefix(ConnectorSession session);
}
