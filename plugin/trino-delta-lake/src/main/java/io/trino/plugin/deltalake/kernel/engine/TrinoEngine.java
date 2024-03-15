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
package io.trino.plugin.deltalake.kernel.engine;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.ParquetHandler;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.type.TypeManager;

public class TrinoEngine
        implements Engine
{
    private final TrinoFileSystem fileSystem;
    private final TypeManager typeManager;

    public TrinoEngine(TrinoFileSystem fileSystem, TypeManager typeManager)
    {
        this.fileSystem = fileSystem;
        this.typeManager = typeManager;
    }

    @Override
    public ExpressionHandler getExpressionHandler()
    {
        return new TrinoExpressionHandler(typeManager);
    }

    @Override
    public JsonHandler getJsonHandler()
    {
        return new TrinoJsonHandler(fileSystem, typeManager);
    }

    @Override
    public FileSystemClient getFileSystemClient()
    {
        return new TrinoFileSystemClient(fileSystem);
    }

    @Override
    public ParquetHandler getParquetHandler()
    {
        return new TrinoParquetHandler(fileSystem, typeManager);
    }
}
