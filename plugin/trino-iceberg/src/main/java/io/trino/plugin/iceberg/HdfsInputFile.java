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
package io.trino.plugin.iceberg;

import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HdfsInputFile
        implements InputFile
{
    private final InputFile delegate;
    private final HdfsEnvironment environment;
    private final ConnectorIdentity identity;

    public HdfsInputFile(Path path, HdfsEnvironment environment, HdfsContext context)
    {
        requireNonNull(path, "path is null");
        this.environment = requireNonNull(environment, "environment is null");
        requireNonNull(context, "context is null");
        try {
            this.delegate = HadoopInputFile.fromPath(path, environment.getFileSystem(context, path), environment.getConfiguration(context, path));
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to create input file: " + path, e);
        }
        this.identity = context.getIdentity();
    }

    @Override
    public long getLength()
    {
        return environment.doAs(identity, delegate::getLength);
    }

    @Override
    public SeekableInputStream newStream()
    {
        // Hack:
        //
        // Trino uses HdfsInputFile instead of HadoopInputFile. This causes the reader in ParquetUtil#fileMetrics
        // to use ParquetInputFile instead of org.apache.parquet.hadoop.util.HadoopInputFile.
        //
        // In this method, if we do not wrap delegate#newStream (a HadoopSeekableInputStream extending DelegatingInputStream) using a
        // wrapper (which is not a DelegatingInputStream), ParquetIO#stream throws away the DelegatingInputStream.
        // The finalizer triggered on this thrown-away HadoopSeekableInputStream ends up closing the delegate as well, which
        // is still in use. That causes errors because of premature closing of the underyling stream in-use.
        //
        // static SeekableInputStream stream(org.apache.iceberg.io.SeekableInputStream stream) {
        //        if (stream instanceof DelegatingInputStream) {
        //            InputStream wrapped = ((DelegatingInputStream)stream).getDelegate();
        //            if (wrapped instanceof FSDataInputStream) {
        //                return HadoopStreams.wrap((FSDataInputStream)wrapped);
        //            }
        //        }
        //
        //        return new ParquetIO.ParquetInputStreamAdapter(stream);
        // }
        //
        // https://github.com/trinodb/trino/issues/5201

        return new HdfsInputStream(environment.doAs(identity, delegate::newStream));
    }

    @Override
    public String location()
    {
        return delegate.location();
    }

    @Override
    public boolean exists()
    {
        return environment.doAs(identity, delegate::exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("delegate", delegate)
                .add("identity", identity)
                .toString();
    }

    private static class HdfsInputStream
            extends SeekableInputStream
    {
        private final SeekableInputStream delegate;

        public HdfsInputStream(SeekableInputStream delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public int read()
                throws IOException
        {
            return delegate.read();
        }

        @Override
        public long getPos()
                throws IOException
        {
            return delegate.getPos();
        }

        @Override
        public void seek(long newPos)
                throws IOException
        {
            delegate.seek(newPos);
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }
    }
}
