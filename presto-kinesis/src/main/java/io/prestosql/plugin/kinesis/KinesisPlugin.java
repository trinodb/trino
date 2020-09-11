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
package io.prestosql.plugin.kinesis;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

public class KinesisPlugin
        implements Plugin
{
    private KinesisConnectorFactory factory;

    public KinesisPlugin() {}

    public KinesisPlugin(KinesisConnectorFactory factory)
    {
        this.factory = factory;
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        if (factory == null) {
            this.factory = new KinesisConnectorFactory();
        }
        return ImmutableList.of(this.factory);
    }
}
