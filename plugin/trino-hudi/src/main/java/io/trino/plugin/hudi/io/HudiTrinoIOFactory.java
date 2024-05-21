package io.trino.plugin.hudi.io;

import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

public class HudiTrinoIOFactory extends HoodieIOFactory {
    public HudiTrinoIOFactory(HoodieStorage storage) {
        super(storage);
    }

    @Override
    public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
        return new HudiTrinoFileReaderFactory(storage);
    }

    @Override
    public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
        throw new UnsupportedOperationException("HudiTrinoIOFactory does not support writers.");
    }

    @Override
    public FileFormatUtils getFileFormatUtils(HoodieFileFormat fileFormat) {
        throw new UnsupportedOperationException("FileFormatUtils not supported in HudiTrinoIOFactory");
    }

    @Override
    public HoodieStorage getStorage(StoragePath storagePath) {
        return storage;
    }

    @Override
    public HoodieStorage getStorage(StoragePath path, boolean enableRetry, long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions, ConsistencyGuard consistencyGuard) {
        return storage;
    }
}
