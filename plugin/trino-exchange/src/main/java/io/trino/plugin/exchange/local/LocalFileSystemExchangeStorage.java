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
package io.trino.plugin.exchange.local;

import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.trino.plugin.exchange.ExchangeStorageWriter;
import io.trino.plugin.exchange.FileSystemExchangeStorage;
import io.trino.spi.TrinoException;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.plugin.exchange.FileSystemExchangeManager.AES;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createFile;

public class LocalFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

    @Override
    public void initialize(URI baseDirectory)
    {
        // no need to do anything for local file system
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        Files.createDirectories(Paths.get(dir.getPath()));
    }

    @Override
    public SliceInput getSliceInput(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        if (secretKey.isPresent()) {
            try {
                final Cipher cipher = Cipher.getInstance(AES);
                cipher.init(Cipher.DECRYPT_MODE, secretKey.get());
                return new InputStreamSliceInput(new CipherInputStream(new FileInputStream(Paths.get(file.getPath()).toFile()), cipher), BUFFER_SIZE_IN_BYTES);
            }
            catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherInputStream " + e.getMessage(), e);
            }
        }
        else {
            return new InputStreamSliceInput(new FileInputStream(Paths.get(file.getPath()).toFile()), BUFFER_SIZE_IN_BYTES);
        }
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        return new LocalExchangeStorageWriter(file, secretKey);
    }

    @Override
    public boolean exists(URI file)
    {
        return Files.exists(Paths.get(file.getPath()));
    }

    @Override
    public void createEmptyFile(URI file)
            throws IOException
    {
        createFile(Paths.get(file.getPath()));
    }

    @Override
    public void deleteRecursively(URI dir)
            throws IOException
    {
        MoreFiles.deleteRecursively(Paths.get(dir.getPath()), ALLOW_INSECURE);
    }

    @Override
    public Stream<URI> listFiles(URI dir)
            throws IOException
    {
        return listPaths(dir, Files::isRegularFile).stream();
    }

    @Override
    public Stream<URI> listDirectories(URI dir)
            throws IOException
    {
        return listPaths(dir, Files::isDirectory).stream();
    }

    @Override
    public long size(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        return Files.size(Paths.get(file.getPath()));
    }

    @Override
    public int getWriteBufferSizeInBytes()
    {
        return BUFFER_SIZE_IN_BYTES;
    }

    @Override
    public void close()
    {
    }

    private static List<URI> listPaths(URI directory, Predicate<Path> predicate)
            throws IOException
    {
        ImmutableList.Builder<URI> builder = ImmutableList.builder();
        try (Stream<Path> dir = Files.list(Paths.get(directory.getPath()))) {
            dir.filter(predicate).map(Path::toUri).forEach(builder::add);
        }
        return builder.build();
    }

    private static class LocalExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private final OutputStream outputStream;

        public LocalExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
                throws FileNotFoundException
        {
            if (secretKey.isPresent()) {
                try {
                    final Cipher cipher = Cipher.getInstance(AES);
                    cipher.init(Cipher.ENCRYPT_MODE, secretKey.get());
                    this.outputStream = new CipherOutputStream(new FileOutputStream(Paths.get(file.getPath()).toFile()), cipher);
                }
                catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherOutputStream " + e.getMessage(), e);
                }
            }
            else {
                this.outputStream = new FileOutputStream(Paths.get(file.getPath()).toFile());
            }
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
                throws IOException
        {
            outputStream.write(slice.getBytes());
            return immediateVoidFuture();
        }

        @Override
        public void close()
                throws IOException
        {
            outputStream.close();
        }
    }
}
