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
package io.trino.loki;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;

import static java.util.Objects.requireNonNull;

public class LokiConnector implements Connector {
    private static final Logger log = Logger.get(LokiConnector.class);

    private final LokiMetadata metadata;
    private final LokiSplitManager splitManager;
    private final LokiRecordSetProvider recordSetProvider;

    @Inject
    public LokiConnector(
            LokiMetadata metadata,
            LokiSplitManager splitManager,
            LokiRecordSetProvider recordSetProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }
}
