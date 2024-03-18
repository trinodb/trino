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
package io.trino.plugin.varada.di;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.util.DefaultFakeConnectorSession;
import io.trino.spi.connector.ConnectorSession;

@Singleton
public class DefaultFakeConnectorSessionProvider
        implements FakeConnectorSessionProvider
{
    @Inject
    public DefaultFakeConnectorSessionProvider() {}

    @Override
    public ConnectorSession get()
    {
        return DefaultFakeConnectorSession.INSTANCE;
    }
}
