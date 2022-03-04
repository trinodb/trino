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
        // Hack: this wrapping is required to circumvent https://github.com/trinodb/trino/issues/5201
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
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            return delegate.read(b, off, len);
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
