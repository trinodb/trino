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
package io.trino.plugin.openlineage.transport.console;

import io.openlineage.client.transports.ConsoleTransport;
import io.trino.plugin.openlineage.transport.OpenLineageTransportCreator;

public class OpenLineageConsoleTransport
        implements OpenLineageTransportCreator
{
    @Override
    public ConsoleTransport buildTransport()
    {
        return new ConsoleTransport();
    }
}
