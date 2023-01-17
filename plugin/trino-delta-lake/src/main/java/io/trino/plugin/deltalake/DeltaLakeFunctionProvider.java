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
package io.trino.plugin.deltalake;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.functions.tablechanges.DeltaLakeTableChangesFunction;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesProcessorProvider;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.ptf.TableFunctionProcessorProvider;

import javax.inject.Inject;

public class DeltaLakeFunctionProvider
        implements FunctionProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeConfig deltaLakeConfig;

    @Inject
    public DeltaLakeFunctionProvider(TrinoFileSystemFactory fileSystemFactory, DeltaLakeConfig deltaLakeConfig)
    {
        this.fileSystemFactory = fileSystemFactory;
        this.deltaLakeConfig = deltaLakeConfig;
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
    {
        return null;
    }

    @Override
    public AggregationImplementation getAggregationImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return null;
    }

    @Override
    public WindowFunctionSupplier getWindowFunctionSupplier(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return null;
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(SchemaFunctionName name)
    {
        if (name.equals(new SchemaFunctionName(DeltaLakeTableChangesFunction.SCHEMA_NAME, DeltaLakeTableChangesFunction.NAME))) {
            return new TableChangesProcessorProvider(fileSystemFactory, deltaLakeConfig);
        }
        return null;
    }
}
