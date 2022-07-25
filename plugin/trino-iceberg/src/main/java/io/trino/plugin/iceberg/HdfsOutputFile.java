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
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HdfsOutputFile
        implements OutputFile
{
    private final OutputFile delegate;
    private final Path path;
    private final HdfsEnvironment environment;
    private final HdfsContext context;

    public HdfsOutputFile(Path path, HdfsEnvironment environment, HdfsContext context)
    {
        this.path = requireNonNull(path, "path is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
        try {
            this.delegate = HadoopOutputFile.fromPath(path, environment.getFileSystem(context, path), environment.getConfiguration(context, path));
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to create output file: " + path.toString(), e);
        }
    }

    @Override
    public PositionOutputStream create()
    {
        return environment.doAs(context.getIdentity(), delegate::create);
    }

    @Override
    public PositionOutputStream createOrOverwrite()
    {
        return environment.doAs(context.getIdentity(), delegate::createOrOverwrite);
    }

    @Override
    public String location()
    {
        return delegate.location();
    }

    @Override
    public InputFile toInputFile()
    {
        return new HdfsInputFile(path, environment, context);
    }
}
