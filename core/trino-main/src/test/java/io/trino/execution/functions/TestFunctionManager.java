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
package io.trino.execution.functions;

import io.trino.FeaturesConfig;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.SystemFunctionBundle;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;

import static io.trino.client.NodeVersion.UNKNOWN;

public class TestFunctionManager
{
    private TestFunctionManager()
    {
    }

    public static FunctionManager createTestingFunctionManager()
    {
        TypeOperators typeOperators = new TypeOperators();
        GlobalFunctionCatalog functionCatalog = new GlobalFunctionCatalog();
        functionCatalog.addFunctions(SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, new BlockTypeOperators(typeOperators), UNKNOWN));

        return new FunctionManager(CatalogServiceProvider.fail(), functionCatalog);
    }
}
