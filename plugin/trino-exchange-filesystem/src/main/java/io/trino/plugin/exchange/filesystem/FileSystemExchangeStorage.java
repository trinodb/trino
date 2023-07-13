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
package io.trino.plugin.exchange.filesystem;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public interface FileSystemExchangeStorage
        extends AutoCloseable
{
    void createDirectories(URI dir)
            throws IOException;

    ExchangeStorageReader createExchangeStorageReader(List<ExchangeSourceFile> sourceFiles, int maxPageStorageSize);

    ExchangeStorageWriter createExchangeStorageWriter(URI file);

    ListenableFuture<Void> createEmptyFile(URI file);

    ListenableFuture<Void> deleteRecursively(List<URI> directories);

    ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir);

    int getWriteBufferSize();

    @Override
    void close()
            throws IOException;
}
