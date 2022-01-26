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

import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.UUID;

public class AzureTransactionLogSynchronizer
        implements TransactionLogSynchronizer
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public AzureTransactionLogSynchronizer(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = hdfsEnvironment;
    }

    // This approach should be compatible with OSS Delta Lake.
    // We assume ADLS Gen2 supports atomic renames which will not overwrite existing files
    @Override
    public void write(ConnectorSession session, String clusterId, Path newLogEntryPath, byte[] entryContents)
    {
        String tmpFileName = newLogEntryPath.getName() + "." + UUID.randomUUID() + ".tmp";
        Path tmpFilePath = new Path(newLogEntryPath.getParent(), tmpFileName);

        FileSystem fs = null;
        try {
            fs = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), newLogEntryPath);
            try (OutputStream outputStream = fs.create(tmpFilePath, false)) {
                outputStream.write(entryContents);
            }
            if (!fs.rename(tmpFilePath, newLogEntryPath)) {
                fs.delete(tmpFilePath, false);
                throw new TransactionConflictException("Conflict detected while writing Transaction Log entry to ADLS");
            }
        }
        catch (IOException e) {
            try {
                if (fs != null && fs.exists(tmpFilePath)) {
                    fs.delete(tmpFilePath, false);
                }
            }
            catch (Exception cleanupException) {
                if (e != cleanupException) {
                    e.addSuppressed(cleanupException);
                }
            }
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isUnsafe()
    {
        return false;
    }
}
