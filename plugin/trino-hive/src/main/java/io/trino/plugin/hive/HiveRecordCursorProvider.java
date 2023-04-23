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
package io.trino.plugin.hive;

import io.trino.filesystem.Location;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public interface HiveRecordCursorProvider
{
    Optional<ReaderRecordCursorWithProjections> createRecordCursor(
            Configuration configuration,
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            TypeManager typeManager,
            boolean s3SelectPushdownEnabled);

    /**
     * A wrapper class for
     * - delegate reader record cursor and
     * - projection information for columns to be returned by the delegate
     * <p>
     * Empty {@param projectedReaderColumns} indicates that the delegate cursor reads the exact same columns provided to
     * it in {@link HiveRecordCursorProvider#createRecordCursor}
     */
    class ReaderRecordCursorWithProjections
    {
        private final RecordCursor recordCursor;
        private final Optional<ReaderColumns> projectedReaderColumns;

        public ReaderRecordCursorWithProjections(RecordCursor recordCursor, Optional<ReaderColumns> projectedReaderColumns)
        {
            this.recordCursor = requireNonNull(recordCursor, "recordCursor is null");
            this.projectedReaderColumns = requireNonNull(projectedReaderColumns, "projectedReaderColumns is null");
        }

        public RecordCursor getRecordCursor()
        {
            return recordCursor;
        }

        public Optional<ReaderColumns> getProjectedReaderColumns()
        {
            return projectedReaderColumns;
        }
    }
}
