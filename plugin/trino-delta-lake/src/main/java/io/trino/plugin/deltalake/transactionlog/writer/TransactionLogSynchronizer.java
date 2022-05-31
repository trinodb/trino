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
package io.trino.plugin.deltalake.transactionlog.writer;

import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

public interface TransactionLogSynchronizer
{
    /**
     * Performs atomic write of transaction log entry file.
     * @throws TransactionConflictException If file cannot be written because of conflict with other transaction
     * @throws RuntimeException If some other unexpected error occurs
     */
    void write(ConnectorSession session, String clusterId, Path newLogEntryPath, byte[] entryContents);

    /**
     * Whether or not writes using this Synchronizer need to be enabled with the "delta.enable-non-concurrent-writes" config property.
     * @return False if collision detection for writes from multiple clusters is supported, else true.
     */
    boolean isUnsafe();
}
