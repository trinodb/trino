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
package io.trino.spi.connector;

public interface ConnectorPageSourceProviderFactory
{
    /**
     * Creates a page source provider for a single table scan.
     *
     * @param memoryContext reports memory retained by state which the provider shares across the page
     *         sources it creates, such as Iceberg equality delete filters. The provider reports the total size of
     *         that state. The engine tolerates concurrent calls from split threads, but the provider is responsible
     *         for reporting a value consistent with the state it holds. The reservation is given up once every page
     *         source created by the provider has been closed.
     */
    ConnectorPageSourceProvider createPageSourceProvider(MemoryContext memoryContext);
}
