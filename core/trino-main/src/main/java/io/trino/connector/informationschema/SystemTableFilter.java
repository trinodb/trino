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
package io.trino.connector.informationschema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataListing.listTables;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class SystemTableFilter<T>
{
    @VisibleForTesting
    public static final int TABLE_COUNT_PER_SCHEMA_THRESHOLD = 3;

    private final String catalogName;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final T catalogColumnReference;
    private final T schemaColumnReference;
    private final T tableColumnReference;
    private final boolean enumerateColumns;
    private final int maxPrefetchedInformationSchemaPrefixes;

    public SystemTableFilter(
            String catalogName,
            Metadata metadata,
            AccessControl accessControl,
            T catalogColumnReference,
            T schemaColumnReference,
            T tableColumnReference,
            boolean enumerateColumns,
            int maxPrefetchedInformationSchemaPrefixes)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.catalogColumnReference = requireNonNull(catalogColumnReference, "catalogColumnReference is null");
        this.schemaColumnReference = requireNonNull(schemaColumnReference, "schemaColumnReference is null");
        this.tableColumnReference = requireNonNull(tableColumnReference, "tableColumnReference is null");
        this.enumerateColumns = enumerateColumns;
        this.maxPrefetchedInformationSchemaPrefixes = maxPrefetchedInformationSchemaPrefixes;
    }

    public static Set<QualifiedTablePrefix> defaultPrefixes(String catalogName)
    {
        return ImmutableSet.of(new QualifiedTablePrefix(catalogName));
    }

    public Set<QualifiedTablePrefix> getPrefixes(ConnectorSession session, TupleDomain<T> constraint, Optional<Predicate<Map<T, NullableValue>>> predicate)
    {
        if (constraint.isNone()) {
            return ImmutableSet.of();
        }

        Optional<Set<String>> catalogs = filterString(constraint, catalogColumnReference);
        if (catalogs.isPresent() && !catalogs.get().contains(catalogName)) {
            return ImmutableSet.of();
        }

        Set<QualifiedTablePrefix> schemaPrefixes = calculatePrefixesWithSchemaName(session, constraint, predicate);
        Set<QualifiedTablePrefix> tablePrefixes = calculatePrefixesWithTableName(session, schemaPrefixes, constraint, predicate);
        verify(tablePrefixes.size() <= maxPrefetchedInformationSchemaPrefixes * TABLE_COUNT_PER_SCHEMA_THRESHOLD, "calculatePrefixesWithTableName returned too many prefixes: %s",
                tablePrefixes.size());
        return tablePrefixes;
    }

    private Set<QualifiedTablePrefix> calculatePrefixesWithSchemaName(
            ConnectorSession connectorSession,
            TupleDomain<T> constraint,
            Optional<Predicate<Map<T, NullableValue>>> predicate)
    {
        Optional<Set<String>> schemas = filterString(constraint, schemaColumnReference);
        if (schemas.isPresent()) {
            return schemas.get().stream()
                    .filter(this::isLowerCase)
                    .filter(schema -> predicate.isEmpty() || predicate.get().test(schemaAsFixedValues(schema)))
                    .map(schema -> new QualifiedTablePrefix(catalogName, schema))
                    .collect(toImmutableSet());
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        return listSchemaNames(session)
                .filter(prefix -> predicate
                        .map(pred -> pred.test(schemaAsFixedValues(prefix.getSchemaName().get())))
                        .orElse(true))
                .collect(toImmutableSet());
    }

    private Set<QualifiedTablePrefix> calculatePrefixesWithTableName(
            ConnectorSession connectorSession,
            Set<QualifiedTablePrefix> schemaPrefixes,
            TupleDomain<T> constraint,
            Optional<Predicate<Map<T, NullableValue>>> predicate)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();

        // when number of tables is >> number of schemas, it's better to fetch whole schemas worth of tables
        // but if there are a lot of schemas and only, say, one or two tables are of interest, then it's faster to actually check tables one by one
        final long schemaPrefixLimit = Math.min(maxPrefetchedInformationSchemaPrefixes, schemaPrefixes.size());
        final long tablePrefixLimit = Math.max(maxPrefetchedInformationSchemaPrefixes, schemaPrefixLimit * TABLE_COUNT_PER_SCHEMA_THRESHOLD);

        // fetch all tables and views
        schemaPrefixes = limitPrefixesCount(schemaPrefixes, schemaPrefixLimit, defaultPrefixes(catalogName));

        Optional<Set<String>> tables = filterString(constraint, tableColumnReference);
        if (tables.isPresent()) {
            Set<QualifiedTablePrefix> tablePrefixes = schemaPrefixes.stream()
                    .peek(prefix -> verify(prefix.asQualifiedObjectName().isEmpty()))
                    .flatMap(prefix -> prefix.getSchemaName()
                            .map(schemaName -> Stream.of(prefix))
                            .orElseGet(() -> listSchemaNames(session)))
                    .flatMap(prefix -> tables.get().stream()
                            .filter(this::isLowerCase)
                            .map(table -> new QualifiedObjectName(catalogName, prefix.getSchemaName().get(), table)))
                    .filter(objectName -> {
                        if (!enumerateColumns ||
                                metadata.isMaterializedView(session, objectName) ||
                                metadata.isView(session, objectName)) {
                            return true;
                        }

                        // This is a columns enumerating table and the object is not a view
                        try {
                            // Table redirection to enumerate columns from target table happens later in
                            // MetadataListing#listTableColumns, but also applying it here to avoid incorrect
                            // filtering in case the source table does not exist or there is a problem with redirection.
                            return metadata.getRedirectionAwareTableHandle(session, objectName).tableHandle().isPresent();
                        }
                        catch (TrinoException e) {
                            if (e.getErrorCode().equals(TABLE_REDIRECTION_ERROR.toErrorCode())) {
                                // Ignore redirection errors for listing, treat as if the table does not exist
                                return false;
                            }

                            throw e;
                        }
                    })
                    .filter(objectName -> predicate.isEmpty() || predicate.get().test(asFixedValues(objectName)))
                    .map(QualifiedObjectName::asQualifiedTablePrefix)
                    .distinct()
                    .limit(tablePrefixLimit + 1)
                    .collect(toImmutableSet());

            return limitPrefixesCount(tablePrefixes, tablePrefixLimit, schemaPrefixes);
        }

        if (predicate.isEmpty() || !enumerateColumns) {
            return schemaPrefixes;
        }

        Set<QualifiedTablePrefix> tablePrefixes = schemaPrefixes.stream()
                .flatMap(prefix -> listTableNames(session, prefix))
                .filter(objectName -> predicate.get().test(asFixedValues(objectName)))
                .map(QualifiedObjectName::asQualifiedTablePrefix)
                .distinct()
                .limit(tablePrefixLimit + 1)
                .collect(toImmutableSet());

        return limitPrefixesCount(tablePrefixes, tablePrefixLimit, schemaPrefixes);
    }

    private Set<QualifiedTablePrefix> limitPrefixesCount(Set<QualifiedTablePrefix> prefixes, long limit, Set<QualifiedTablePrefix> fallback)
    {
        if (prefixes.size() <= limit) {
            return prefixes;
        }
        // in case of high number of prefixes it is better to populate all data and then filter
        // TODO this may cause re-running the above filtering upon next applyFilter
        return fallback;
    }

    private Stream<QualifiedTablePrefix> listSchemaNames(Session session)
    {
        return listSchemas(session, metadata, accessControl, catalogName, Optional.empty())
                .stream()
                .map(schema -> new QualifiedTablePrefix(catalogName, schema));
    }

    private Stream<QualifiedObjectName> listTableNames(Session session, QualifiedTablePrefix prefix)
    {
        return listTables(session, metadata, accessControl, prefix)
                .stream()
                .map(table -> new QualifiedObjectName(catalogName, table.getSchemaName(), table.getTableName()));
    }

    private Optional<Set<String>> filterString(TupleDomain<T> constraint, T column)
    {
        if (constraint.isNone()) {
            return Optional.of(ImmutableSet.of());
        }

        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return Optional.empty();
        }

        if (domain.isSingleValue()) {
            return Optional.of(ImmutableSet.of(((Slice) domain.getSingleValue()).toStringUtf8()));
        }
        if (domain.getValues() instanceof EquatableValueSet) {
            Collection<Object> values = ((EquatableValueSet) domain.getValues()).getValues();
            return Optional.of(values.stream()
                    .map(Slice.class::cast)
                    .map(Slice::toStringUtf8)
                    .collect(toImmutableSet()));
        }
        if (domain.getValues() instanceof SortedRangeSet) {
            ImmutableSet.Builder<String> result = ImmutableSet.builder();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                if (!range.isSingleValue()) {
                    return Optional.empty();
                }

                result.add(((Slice) range.getSingleValue()).toStringUtf8());
            }

            return Optional.of(result.build());
        }
        return Optional.empty();
    }

    private Map<T, NullableValue> schemaAsFixedValues(String schema)
    {
        return ImmutableMap.of(schemaColumnReference, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }

    private Map<T, NullableValue> asFixedValues(QualifiedObjectName objectName)
    {
        return ImmutableMap.of(
                catalogColumnReference, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getCatalogName())),
                schemaColumnReference, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getSchemaName())),
                tableColumnReference, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getObjectName())));
    }

    private boolean isLowerCase(String value)
    {
        return value.toLowerCase(ENGLISH).equals(value);
    }
}
