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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.plugin.inference.ptf.DescribeModelProcessorProvider;
import io.trino.plugin.inference.ptf.DescribeModelTableFunction;
import io.trino.plugin.inference.ptf.PredictProcessorProvider;
import io.trino.plugin.inference.ptf.PredictTableFunction;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class InferenceModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(InferenceConnector.class).in(Scopes.SINGLETON);
        binder.bind(InferenceMetadata.class).in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(InferenceFunctionProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(InferenceConfig.class);
        httpClientBinder(binder).bindHttpClient("inference", ForInference.class);
        binder.bind(OpenInferenceClient.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OpenInferenceClient.class).withGeneratedName();

        binder.bind(ConnectorSplitManager.class).to(InferenceSplitManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().to(PredictTableFunction.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().to(DescribeModelTableFunction.class).in(Scopes.SINGLETON);
        binder.bind(PredictProcessorProvider.class).in(Scopes.SINGLETON);
        binder.bind(DescribeModelProcessorProvider.class).in(Scopes.SINGLETON);
    }
}
