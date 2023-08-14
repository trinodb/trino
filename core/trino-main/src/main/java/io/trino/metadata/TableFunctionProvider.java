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
package io.trino.metadata;

import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import java.util.Optional;

public interface TableFunctionProvider
{
    /**
     * Provides specific TableFunction
     * with TableFunctionProcessorProvider and ConnectorSplitSource implementation
     * for concrete function handle
     */
    Optional<TableFunction> get(ConnectorTableFunctionHandle functionHandle);

    interface TableFunction
    {
        default TableFunctionProcessorProvider getTableFunctionProcessorProvider()
        {
            throw new UnsupportedOperationException("%s does not provide table function processor provider".formatted(getClass().getName()));
        }

        default ConnectorSplitSource getSplitSource()
        {
            throw new UnsupportedOperationException("%s does not provide connector split source".formatted(getClass().getName()));
        }
    }
}
