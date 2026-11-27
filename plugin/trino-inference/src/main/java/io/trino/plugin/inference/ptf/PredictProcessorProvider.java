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
package io.trino.plugin.inference.ptf;

import com.google.inject.Inject;
import io.trino.plugin.inference.InferenceConfig;
import io.trino.plugin.inference.OpenInferenceClient;
import io.trino.plugin.inference.ptf.PredictTableFunction.PredictFunctionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import static java.util.Objects.requireNonNull;

public class PredictProcessorProvider
        implements TableFunctionProcessorProvider
{
    private final OpenInferenceClient client;
    private final int maxBatchSize;

    @Inject
    public PredictProcessorProvider(OpenInferenceClient client, InferenceConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.maxBatchSize = config.getMaxBatchSize();
    }

    @Override
    public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        return new PredictDataProcessor(client, (PredictFunctionHandle) handle, maxBatchSize);
    }
}
