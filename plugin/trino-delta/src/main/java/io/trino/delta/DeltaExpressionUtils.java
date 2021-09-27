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
package io.trino.delta;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static io.trino.delta.DeltaErrorCode.DELTA_INVALID_PARTITION_VALUE;
import static io.trino.delta.DeltaErrorCode.DELTA_UNSUPPORTED_COLUMN_TYPE;
import static io.trino.delta.DeltaSessionProperties.isPartitionPruningEnabled;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

public class DeltaExpressionUtils
{
    private DeltaExpressionUtils()
    {
    }

    /**
     * Split the predicate into partition and regular column predicates
     */
    public static List<TupleDomain<ColumnHandle>> splitPredicate(
            TupleDomain<ColumnHandle> predicate)
    {
        Map<ColumnHandle, Domain> partitionColumnPredicates = new HashMap<>();
        Map<ColumnHandle, Domain> regularColumnPredicates = new HashMap<>();

        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        if (domains.isPresent()) {
            domains.get().entrySet().stream()
                    .forEach(domainPair -> {
                        DeltaColumnHandle columnHandle = (DeltaColumnHandle) domainPair.getKey();
                        if (columnHandle.getColumnType() == PARTITION) {
                            partitionColumnPredicates.put(domainPair.getKey(), domainPair.getValue());
                        }
                        else {
                            regularColumnPredicates.put(domainPair.getKey(), domainPair.getValue());
                        }
                    });
        }

        return ImmutableList.of(
                TupleDomain.withColumnDomains(partitionColumnPredicates),
                TupleDomain.withColumnDomains(regularColumnPredicates));
    }

    /**
     * Utility method to
     * 1) remove the null value partition values. These null values cause problems later
     *    when used with Guava Immutable map structures.
     * 2) convert column names to lower case. Trino converts the column names to lowercase.
     */
    public static Map<String, String> sanitizePartitionValues(Map<String, String> partitionValues)
    {
        return partitionValues.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(entry -> entry.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));
    }

    /**
     * Utility method that takes an iterator of {@link AddFile}s and a predicate and returns an iterator of {@link AddFile}s
     * that satisfy the predicate (predicate evaluates to a deterministic NO)
     */
    public static CloseableIterator<AddFile> iterateWithPartitionPruning(
            ConnectorSession session,
            CloseableIterator<AddFile> inputIterator,
            TupleDomain<DeltaColumnHandle> predicate,
            TypeManager typeManager)
    {
        TupleDomain<String> partitionPredicate = extractPartitionColumnsPredicate(predicate);
        if (partitionPredicate.isAll() /* no partition filter */ || !isPartitionPruningEnabled(session)) {
            return inputIterator;
        }

        if (partitionPredicate.isNone()) {
            // nothing passes the partition predicate, return empty iterator
            return new CloseableIterator<AddFile>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public AddFile next()
                {
                    throw new NoSuchElementException();
                }

                @Override
                public void close()
                        throws IOException
                {
                    inputIterator.close();
                }
            };
        }

        List<DeltaColumnHandle> partitionColumns =
                predicate.getDomains().get().entrySet().stream()
                        .filter(entry -> entry.getKey().getColumnType() == PARTITION)
                        .map(entry -> entry.getKey())
                        .collect(Collectors.toList());

        return new CloseableIterator<>()
        {
            private AddFile nextItem;

            @Override
            public boolean hasNext()
            {
                if (nextItem != null) {
                    return true;
                }

                while (inputIterator.hasNext()) {
                    AddFile nextFile = inputIterator.next();
                    if (evaluatePartitionPredicate(partitionPredicate, partitionColumns, typeManager, nextFile)) {
                        nextItem = nextFile;
                        break;
                    }
                }

                return nextItem != null;
            }

            @Override
            public AddFile next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("there are no more files");
                }
                AddFile toReturn = nextItem;
                nextItem = null;
                return toReturn;
            }

            @Override
            public void close()
                    throws IOException
            {
                inputIterator.close();
            }
        };
    }

    private static TupleDomain<String> extractPartitionColumnsPredicate(TupleDomain<DeltaColumnHandle> predicate)
    {
        return predicate
                .filter(((columnHandle, domain) -> columnHandle.getColumnType() == PARTITION))
                .transformKeys(DeltaColumnHandle::getName);
    }

    private static boolean evaluatePartitionPredicate(
            TupleDomain<String> partitionPredicate,
            List<DeltaColumnHandle> partitionColumns,
            TypeManager typeManager,
            AddFile addFile)
    {
        Map<String, String> partitionValues = sanitizePartitionValues(addFile.getPartitionValues());
        for (DeltaColumnHandle partitionColumn : partitionColumns) {
            String columnName = partitionColumn.getName();
            String partitionValue = partitionValues.get(columnName.toLowerCase(Locale.ROOT));
            Domain domain = getDomain(partitionColumn, partitionValue, typeManager, addFile.getPath());
            Domain columnPredicate = partitionPredicate.getDomains().get().get(columnName);

            if (columnPredicate == null) {
                continue; // there is no predicate on this column
            }

            if (columnPredicate.intersect(domain).isNone()) {
                return false;
            }
        }

        return true;
    }

    private static Domain getDomain(DeltaColumnHandle columnHandle, String partitionValue, TypeManager typeManager, String filePath)
    {
        Type type = typeManager.getType(columnHandle.getDataType());
        if (partitionValue == null) {
            return Domain.onlyNull(type);
        }

        String typeBase = columnHandle.getDataType().getBase();
        try {
            switch (typeBase) {
                case StandardTypes.TINYINT:
                case StandardTypes.SMALLINT:
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                    Long intValue = parseLong(partitionValue);
                    return Domain.create(ValueSet.of(type, intValue), false);
                case StandardTypes.REAL:
                    Long realValue = (long) floatToRawIntBits(parseFloat(partitionValue));
                    return Domain.create(ValueSet.of(type, realValue), false);
                case StandardTypes.DOUBLE:
                    Long doubleValue = doubleToRawLongBits(parseDouble(partitionValue));
                    return Domain.create(ValueSet.of(type, doubleValue), false);
                case StandardTypes.VARCHAR:
                case StandardTypes.VARBINARY:
                    Slice sliceValue = utf8Slice(partitionValue);
                    return Domain.create(ValueSet.of(type, sliceValue), false);
                case StandardTypes.DATE:
                    Long dateValue = Date.valueOf(partitionValue).toLocalDate().toEpochDay();
                    return Domain.create(ValueSet.of(type, dateValue), false);
                case StandardTypes.TIMESTAMP:
                    Long timestampValue = Timestamp.valueOf(partitionValue).getTime(); // convert to millis
                    return Domain.create(ValueSet.of(type, timestampValue), false);
                case StandardTypes.BOOLEAN:
                    Boolean booleanValue = Boolean.valueOf(partitionValue);
                    return Domain.create(ValueSet.of(type, booleanValue), false);
                default:
                    throw new TrinoException(DELTA_UNSUPPORTED_COLUMN_TYPE,
                            format("Unsupported data type '%s' for partition column %s", columnHandle.getDataType(), columnHandle.getName()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(DELTA_INVALID_PARTITION_VALUE,
                    format("Can not parse partition value '%s' of type '%s' for partition column '%s' in file '%s'",
                            partitionValue, columnHandle.getDataType(), columnHandle.getName(), filePath));
        }
    }
}
