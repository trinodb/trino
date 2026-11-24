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
package io.trino.plugin.inference;

import com.google.inject.Inject;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionProcessorProvider;
import io.trino.plugin.inference.ptf.DescribeModelProcessorProvider;
import io.trino.plugin.inference.ptf.DescribeModelTableFunction.DescribeModelFunctionHandle;
import io.trino.plugin.inference.ptf.PredictProcessorProvider;
import io.trino.plugin.inference.ptf.PredictTableFunction.PredictFunctionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import static java.util.Objects.requireNonNull;

public class InferenceFunctionProvider
        implements FunctionProvider
{
    private final PredictProcessorProvider predictProcessorProvider;
    private final DescribeModelProcessorProvider describeModelProcessorProvider;

    @Inject
    public InferenceFunctionProvider(PredictProcessorProvider predictProcessorProvider, DescribeModelProcessorProvider describeModelProcessorProvider)
    {
        this.predictProcessorProvider = requireNonNull(predictProcessorProvider, "predictProcessorProvider is null");
        this.describeModelProcessorProvider = requireNonNull(describeModelProcessorProvider, "describeModelProcessorProvider is null");
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof PredictFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProvider(predictProcessorProvider, getClass().getClassLoader());
        }
        if (functionHandle instanceof DescribeModelFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProvider(describeModelProcessorProvider, getClass().getClassLoader());
        }
        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }
}
