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
package io.trino.plugin.lance;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LanceSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(LanceSplitManager.class);
    private final LanceReader lanceReader;

    @Inject
    public LanceSplitManager(LanceReader lanceReader)
    {
        this.lanceReader = requireNonNull(lanceReader, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorTableHandle tableHandle, DynamicFilter dynamicFilter, Constraint constraint)
    {
        List<String> fragments = Collections.emptyList();
        // TODO: add support for splits based on fragments, now entire table is only 1 fragment, thus one split
        // b/c fragments are coming from datasets.getFragments()
        // we need to prevent dataset changes between plan gen (this thread) and actual pagesource creation (runtime)
        // so we need to keep version
        return new FixedSplitSource(new LanceSplit(fragments));
    }
}
