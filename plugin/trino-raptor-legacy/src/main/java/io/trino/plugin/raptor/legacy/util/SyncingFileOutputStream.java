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
package io.trino.plugin.raptor.legacy.util;

import com.google.common.io.Closer;
import io.airlift.slice.XxHash64;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SyncingFileOutputStream
        extends FileOutputStream
{
    private final byte[] oneByte = new byte[1];
    private final XxHash64 hash = new XxHash64();
    private final File file;
    private boolean closed;

    public SyncingFileOutputStream(File file)
            throws FileNotFoundException
    {
        super(file);
        this.file = file;
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        super.write(b, off, len);
        hash.update(b, off, len);
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Override
    public void write(int b)
            throws IOException
    {
        oneByte[0] = (byte) b;
        write(oneByte, 0, 1);
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        try (Closer closer = Closer.create()) {
            closer.register(super::close);
            closer.register(() -> getFD().sync());
            closer.register(this::flush);
        }

        // extremely paranoid code to detect a broken local file system
        try (InputStream in = new FileInputStream(file)) {
            if (hash.hash() != XxHash64.hash(in)) {
                throw new IOException("File is corrupt after write");
            }
        }
    }
}
