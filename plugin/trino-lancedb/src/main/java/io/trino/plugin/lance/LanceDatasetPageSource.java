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
package io.trino.plugin.lance;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ipc.LanceScanner;
import io.airlift.log.Logger;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.plugin.lance.internal.ScannerFactory;
import org.apache.arrow.memory.BufferAllocator;

public class LanceDatasetPageSource
        extends LanceBasePageSource
{
    private static final Logger log = Logger.get(LanceDatasetPageSource.class);

    public LanceDatasetPageSource(LanceReader lanceReader, LanceTableHandle tableHandle, int maxReadRowsRetries)
    {
        super(lanceReader, tableHandle, maxReadRowsRetries);
    }

    @Override
    public ScannerFactory getScannerFactory()
    {
        return new DatasetScannerFactory();
    }

    public static class DatasetScannerFactory
            implements ScannerFactory
    {
        private Dataset lanceDataset;
        private LanceScanner lanceScanner;

        @Override
        public LanceScanner open(String tablePath, BufferAllocator allocator)
        {
            this.lanceDataset = Dataset.open(tablePath, allocator);
            this.lanceScanner = lanceDataset.newScan();
            return lanceScanner;
        }

        @Override
        public void close()
        {
            try {
                if (lanceScanner != null) {
                    lanceScanner.close();
                }
            }
            catch (Exception e) {
                log.warn("error while closing lance scanner, Exception: %s", e.getMessage());
            }
            if (lanceDataset != null) {
                lanceDataset.close();
            }
        }
    }
}
