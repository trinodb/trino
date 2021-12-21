package io.trino.plugin.exchange;
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

import io.airlift.slice.SliceInput;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;

public interface FileSystemExchangeStorage
        extends AutoCloseable
{
    void initialize(URI baseDirectory);

    void createDirectories(URI dir) throws IOException;

    SliceInput getSliceInput(URI file, Optional<SecretKey> secretKey) throws IOException;

    ExchangeStorageWriter createExchangeStorageWriter(URI file, Optional<SecretKey> secretKey) throws IOException;

    boolean exists(URI file) throws IOException;

    void createEmptyFile(URI file) throws IOException;

    void deleteRecursively(URI dir) throws IOException;

    Stream<URI> listFiles(URI dir) throws IOException;

    Stream<URI> listDirectories(URI dir) throws IOException;

    long size(URI file, Optional<SecretKey> secretKey) throws IOException;

    int getWriteBufferSizeInBytes();

    @Override
    void close() throws IOException;
}
