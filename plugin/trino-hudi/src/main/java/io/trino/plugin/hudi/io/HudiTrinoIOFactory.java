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
package io.trino.plugin.hudi.io;

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

public class HudiTrinoIOFactory
        extends HoodieIOFactory
{
    public HudiTrinoIOFactory(HoodieStorage storage)
    {
        super(storage);
    }

    @Override
    public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType)
    {
        return new HudiTrinoFileReaderFactory(storage);
    }

    @Override
    public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType)
    {
        throw new UnsupportedOperationException("HudiTrinoIOFactory does not support writers.");
    }

    @Override
    public FileFormatUtils getFileFormatUtils(HoodieFileFormat fileFormat)
    {
        throw new UnsupportedOperationException("FileFormatUtils not supported in HudiTrinoIOFactory");
    }

    @Override
    public HoodieStorage getStorage(StoragePath storagePath)
    {
        return storage;
    }

    @Override
    public HoodieStorage getStorage(StoragePath path, boolean enableRetry, long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions, ConsistencyGuard consistencyGuard)
    {
        return storage;
    }
}
