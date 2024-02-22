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

import java.io.Closeable;
import java.util.List;

public interface ConnectorAlternativePageSourceProvider
        extends Closeable
{
    /**
     * Creates {@link ConnectorPageSource} for the alternative chosen by {@link ConnectorAlternativeChooser#chooseAlternative(ConnectorSession, ConnectorSplit, List)}.
     * {@link ConnectorTableHandle} was selected when the alternative was chosen, so it's not provided as an argument here.
     */
    ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            boolean splitAddressEnforced);

    /**
     * Closes this page source provider.
     * There will be no more calls to the {@link #createPageSource} once this method is called.
     * Connector can use this to release any resources or locks associated with this provider with the caveat that there may be still
     * not closed and being used {@link ConnectorPageSource} created by this object.
     * Trino will always call this method.
     */
    @Override
    default void close()
    {
    }
}
