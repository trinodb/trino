package io.trino.plugin.lance.internal;

import com.lancedb.lance.ipc.LanceScanner;
import org.apache.arrow.memory.BufferAllocator;

public interface ScannerFactory
{
    LanceScanner open(String tablePath, BufferAllocator allocator);

    void close();
}
