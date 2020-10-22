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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public interface HivePageSourceFactory
{
    Optional<ReaderPageSourceWithProjections> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo);

    /**
     * A wrapper class for
     * - delegate reader page source and
     * - projection information for columns to be returned by the delegate
     * <p>
     * Empty {@param projectedReaderColumns} indicates that the delegate page source reads the exact same columns provided to
     * it in {@link HivePageSourceFactory#createPageSource}
     */
    class ReaderPageSourceWithProjections
    {
        private final ConnectorPageSource connectorPageSource;
        private final Optional<ReaderProjections> projectedReaderColumns;

        public ReaderPageSourceWithProjections(ConnectorPageSource connectorPageSource, Optional<ReaderProjections> projectedReaderColumns)
        {
            this.connectorPageSource = requireNonNull(connectorPageSource, "connectorPageSource is null");
            this.projectedReaderColumns = requireNonNull(projectedReaderColumns, "projectedReaderColumns is null");
        }

        public ConnectorPageSource getConnectorPageSource()
        {
            return connectorPageSource;
        }

        public Optional<ReaderProjections> getProjectedReaderColumns()
        {
            return projectedReaderColumns;
        }

        public static ReaderPageSourceWithProjections noProjectionAdaptation(ConnectorPageSource connectorPageSource)
        {
            return new ReaderPageSourceWithProjections(connectorPageSource, Optional.empty());
        }
    }
}
