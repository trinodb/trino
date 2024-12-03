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

import com.google.inject.Inject;
import io.trino.spi.NodeManager;
import org.apache.iceberg.EnvironmentContext;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.EnvironmentContext.ENGINE_NAME;
import static org.apache.iceberg.EnvironmentContext.ENGINE_VERSION;

public class IcebergEnvironmentContext
{
    @Inject
    public IcebergEnvironmentContext(NodeManager nodeManager)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        EnvironmentContext.put(ENGINE_NAME, "trino");
        EnvironmentContext.put(ENGINE_VERSION, nodeManager.getCurrentNode().getVersion());
    }
}
