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
package io.trino.plugin.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.DBRef;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.plugin.mongodb.ptf.Query.parseFilter;
import static io.trino.spi.HostAddress.fromParts;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MongoSession
{
    private static final Logger log = Logger.get(MongoSession.class);
    private static final Set<String> SYSTEM_DATABASES = Set.of("admin", "local", "config");
    private static final List<String> SYSTEM_TABLES = Arrays.asList("system.indexes", "system.users", "system.version", "system.views");

    private static final String TABLE_NAME_KEY = "table";
    private static final String COMMENT_KEY = "comment";
    private static final String FIELDS_KEY = "fields";
    private static final String FIELDS_NAME_KEY = "name";
    private static final String FIELDS_TYPE_KEY = "type";
    private static final String FIELDS_HIDDEN_KEY = "hidden";

    private static final String AND_OP = "$and";
    private static final String OR_OP = "$or";

    private static final String EQ_OP = "$eq";
    private static final String NOT_EQ_OP = "$ne";
    private static final String GTE_OP = "$gte";
    private static final String GT_OP = "$gt";
    private static final String LT_OP = "$lt";
    private static final String LTE_OP = "$lte";
    private static final String IN_OP = "$in";

    public static final String DATABASE_NAME = "databaseName";
    public static final String COLLECTION_NAME = "collectionName";
    public static final String ID = "id";

    // The 'simple' locale is the default collection in MongoDB. The locale doesn't allow specifying other fields (e.g. numericOrdering)
    // https://www.mongodb.com/docs/manual/reference/collation/
    private static final Collation SIMPLE_COLLATION = Collation.builder().locale("simple").build();
    private static final Map<String, Object> AUTHORIZED_LIST_COLLECTIONS_COMMAND = ImmutableMap.<String, Object>builder()
            .put("listCollections", 1.0)
            .put("nameOnly", true)
            .put("authorizedCollections", true)
            .buildOrThrow();

    private final TypeManager typeManager;
    private final MongoClient client;

    private final String schemaCollection;
    private final boolean caseInsensitiveNameMatching;
    private final int cursorBatchSize;

    private final Cache<SchemaTableName, MongoTable> tableCache;
    private final String implicitPrefix;

    public MongoSession(TypeManager typeManager, MongoClient client, MongoClientConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.client = requireNonNull(client, "client is null");
        this.schemaCollection = requireNonNull(config.getSchemaCollection(), "config.getSchemaCollection() is null");
        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        this.cursorBatchSize = config.getCursorBatchSize();
        this.implicitPrefix = requireNonNull(config.getImplicitRowFieldPrefix(), "config.getImplicitRowFieldPrefix() is null");

        this.tableCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(1, MINUTES)  // TODO: Configure
                .build();
    }

    public void shutdown()
    {
        client.close();
    }

    public List<HostAddress> getAddresses()
    {
        return client.getClusterDescription().getServerDescriptions().stream()
                .map(description -> fromParts(description.getAddress().getHost(), description.getAddress().getPort()))
                .collect(toImmutableList());
    }

    public List<String> getAllSchemas()
    {
        return Streams.stream(listDatabaseNames())
                .filter(schema -> !SYSTEM_DATABASES.contains(schema))
                .map(schema -> schema.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    public void createSchema(String schemaName)
    {
        // Put an empty schema collection because MongoDB doesn't support a database without collections
        client.getDatabase(schemaName).createCollection(schemaCollection);
    }

    public void dropSchema(String schemaName)
    {
        client.getDatabase(toRemoteSchemaName(schemaName)).drop();
    }

    public Set<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        String schemaName = toRemoteSchemaName(schema);
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();

        builder.addAll(ImmutableList.copyOf(listCollectionNames(schemaName)).stream()
                .filter(name -> !name.equals(schemaCollection))
                .filter(name -> !SYSTEM_TABLES.contains(name))
                .collect(toSet()));
        builder.addAll(getTableMetadataNames(schema));

        return builder.build();
    }

    public MongoTable getTable(SchemaTableName tableName)
            throws TableNotFoundException
    {
        try {
            return tableCache.get(tableName, () -> loadTableSchema(tableName));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e);
        }
    }

    public void createTable(RemoteTableName name, List<MongoColumnHandle> columns, Optional<String> comment)
    {
        if (getAllSchemas().stream().noneMatch(schemaName -> schemaName.equalsIgnoreCase(name.getDatabaseName()))) {
            throw new SchemaNotFoundException(name.getDatabaseName());
        }
        createTableMetadata(name, columns, comment);
        client.getDatabase(name.getDatabaseName()).createCollection(name.getCollectionName());
    }

    public void dropTable(RemoteTableName remoteTableName)
    {
        deleteTableMetadata(remoteTableName);
        getCollection(remoteTableName).drop();

        tableCache.invalidate(new SchemaTableName(remoteTableName.getDatabaseName(), remoteTableName.getCollectionName()));
    }

    public void setTableComment(MongoTableHandle table, Optional<String> comment)
    {
        String remoteSchemaName = table.getRemoteTableName().getDatabaseName();
        String remoteTableName = table.getRemoteTableName().getCollectionName();

        Document metadata = getTableMetadata(remoteSchemaName, remoteTableName);
        metadata.append(COMMENT_KEY, comment.orElse(null));

        client.getDatabase(remoteSchemaName).getCollection(schemaCollection)
                .findOneAndReplace(new Document(TABLE_NAME_KEY, remoteTableName), metadata);

        tableCache.invalidate(table.getSchemaTableName());
    }

    public void setColumnComment(MongoTableHandle table, String columnName, Optional<String> comment)
    {
        String remoteSchemaName = table.getRemoteTableName().getDatabaseName();
        String remoteTableName = table.getRemoteTableName().getCollectionName();

        Document metadata = getTableMetadata(remoteSchemaName, remoteTableName);

        ImmutableList.Builder<Document> columns = ImmutableList.builder();
        for (Document column : getColumnMetadata(metadata)) {
            if (column.getString(FIELDS_NAME_KEY).equals(columnName)) {
                column.append(COMMENT_KEY, comment.orElse(null));
            }
            columns.add(column);
        }

        metadata.append(FIELDS_KEY, columns.build());

        client.getDatabase(remoteSchemaName).getCollection(schemaCollection)
                .findOneAndReplace(new Document(TABLE_NAME_KEY, remoteTableName), metadata);

        tableCache.invalidate(table.getSchemaTableName());
    }

    public void renameTable(MongoTableHandle table, SchemaTableName newName)
    {
        String oldSchemaName = table.getRemoteTableName().getDatabaseName();
        String oldTableName = table.getRemoteTableName().getCollectionName();
        String newSchemaName = toRemoteSchemaName(newName.getSchemaName());

        // Schema collection should always have the source table definition
        MongoCollection<Document> oldSchema = client.getDatabase(oldSchemaName).getCollection(schemaCollection);
        Document tableDefinition = oldSchema.findOneAndDelete(new Document(TABLE_NAME_KEY, oldTableName));
        requireNonNull(tableDefinition, "Table definition not found in schema collection: " + oldTableName);

        MongoCollection<Document> newSchema = client.getDatabase(newSchemaName).getCollection(schemaCollection);
        tableDefinition.append(TABLE_NAME_KEY, newName.getTableName());
        newSchema.insertOne(tableDefinition);

        // Need to check explicitly because the old collection may not exist when it doesn't have any data
        if (collectionExists(client.getDatabase(oldSchemaName), oldTableName)) {
            getCollection(table.getRemoteTableName()).renameCollection(new MongoNamespace(newSchemaName, newName.getTableName()));
        }

        tableCache.invalidate(table.getSchemaTableName());
    }

    public void addColumn(MongoTableHandle table, ColumnMetadata columnMetadata)
    {
        String remoteSchemaName = table.getRemoteTableName().getDatabaseName();
        String remoteTableName = table.getRemoteTableName().getCollectionName();

        Document metadata = getTableMetadata(remoteSchemaName, remoteTableName);

        List<Document> columns = new ArrayList<>(getColumnMetadata(metadata));

        Document newColumn = new Document();
        newColumn.append(FIELDS_NAME_KEY, columnMetadata.getName());
        newColumn.append(FIELDS_TYPE_KEY, columnMetadata.getType().getTypeSignature().toString());
        newColumn.append(COMMENT_KEY, columnMetadata.getComment());
        newColumn.append(FIELDS_HIDDEN_KEY, false);
        columns.add(newColumn);

        metadata.append(FIELDS_KEY, columns);

        MongoDatabase db = client.getDatabase(remoteSchemaName);
        MongoCollection<Document> schema = db.getCollection(schemaCollection);
        schema.findOneAndReplace(new Document(TABLE_NAME_KEY, remoteTableName), metadata);

        tableCache.invalidate(table.getSchemaTableName());
    }

    public void renameColumn(MongoTableHandle table, String source, String target)
    {
        String remoteSchemaName = table.getRemoteTableName().getDatabaseName();
        String remoteTableName = table.getRemoteTableName().getCollectionName();

        Document metadata = getTableMetadata(remoteSchemaName, remoteTableName);

        List<Document> columns = getColumnMetadata(metadata).stream()
                .map(document -> {
                    if (document.getString(FIELDS_NAME_KEY).equals(source)) {
                        document.put(FIELDS_NAME_KEY, target);
                    }
                    return document;
                })
                .collect(toImmutableList());

        metadata.append(FIELDS_KEY, columns);

        MongoDatabase database = client.getDatabase(remoteSchemaName);
        MongoCollection<Document> schema = database.getCollection(schemaCollection);
        schema.findOneAndReplace(new Document(TABLE_NAME_KEY, remoteTableName), metadata);

        database.getCollection(remoteTableName)
                .updateMany(Filters.empty(), Updates.rename(source, target));

        tableCache.invalidate(table.getSchemaTableName());
    }

    public void dropColumn(MongoTableHandle table, String columnName)
    {
        String remoteSchemaName = table.getRemoteTableName().getDatabaseName();
        String remoteTableName = table.getRemoteTableName().getCollectionName();

        Document metadata = getTableMetadata(remoteSchemaName, remoteTableName);

        List<Document> columns = getColumnMetadata(metadata).stream()
                .filter(document -> !document.getString(FIELDS_NAME_KEY).equals(columnName))
                .collect(toImmutableList());

        metadata.append(FIELDS_KEY, columns);

        MongoDatabase database = client.getDatabase(remoteSchemaName);
        MongoCollection<Document> schema = database.getCollection(schemaCollection);
        schema.findOneAndReplace(new Document(TABLE_NAME_KEY, remoteTableName), metadata);

        database.getCollection(remoteTableName)
                .updateMany(Filters.empty(), Updates.unset(columnName));

        tableCache.invalidate(table.getSchemaTableName());
    }

    public void setColumnType(MongoTableHandle table, String columnName, Type type)
    {
        String remoteSchemaName = table.getRemoteTableName().getDatabaseName();
        String remoteTableName = table.getRemoteTableName().getCollectionName();

        Document metadata = getTableMetadata(remoteSchemaName, remoteTableName);

        List<Document> columns = getColumnMetadata(metadata).stream()
                .map(document -> {
                    if (document.getString(FIELDS_NAME_KEY).equals(columnName)) {
                        document.put(FIELDS_TYPE_KEY, type.getTypeSignature().toString());
                        return document;
                    }
                    return document;
                })
                .collect(toImmutableList());

        metadata.replace(FIELDS_KEY, columns);

        client.getDatabase(remoteSchemaName).getCollection(schemaCollection)
                .findOneAndReplace(new Document(TABLE_NAME_KEY, remoteTableName), metadata);

        tableCache.invalidate(table.getSchemaTableName());
    }

    private MongoTable loadTableSchema(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        RemoteTableName remoteSchemaTableName = toRemoteSchemaTableName(schemaTableName);
        String remoteSchemaName = remoteSchemaTableName.getDatabaseName();
        String remoteTableName = remoteSchemaTableName.getCollectionName();

        Document tableMeta = getTableMetadata(remoteSchemaName, remoteTableName);

        ImmutableList.Builder<MongoColumnHandle> columnHandles = ImmutableList.builder();

        for (Document columnMetadata : getColumnMetadata(tableMeta)) {
            MongoColumnHandle columnHandle = buildColumnHandle(columnMetadata);
            columnHandles.add(columnHandle);
        }

        MongoTableHandle tableHandle = new MongoTableHandle(schemaTableName, remoteSchemaTableName, Optional.empty());
        return new MongoTable(tableHandle, columnHandles.build(), getIndexes(remoteSchemaName, remoteTableName), getComment(tableMeta));
    }

    private MongoColumnHandle buildColumnHandle(Document columnMeta)
    {
        String name = columnMeta.getString(FIELDS_NAME_KEY);
        String typeString = columnMeta.getString(FIELDS_TYPE_KEY);
        boolean hidden = columnMeta.getBoolean(FIELDS_HIDDEN_KEY, false);
        String comment = columnMeta.getString(COMMENT_KEY);

        Type type = typeManager.fromSqlType(typeString);

        return new MongoColumnHandle(name, type, hidden, Optional.ofNullable(comment));
    }

    private List<Document> getColumnMetadata(Document doc)
    {
        if (!doc.containsKey(FIELDS_KEY)) {
            return ImmutableList.of();
        }

        return (List<Document>) doc.get(FIELDS_KEY);
    }

    private static Optional<String> getComment(Document doc)
    {
        return Optional.ofNullable(doc.getString(COMMENT_KEY));
    }

    public MongoCollection<Document> getCollection(RemoteTableName remoteTableName)
    {
        return client.getDatabase(remoteTableName.getDatabaseName()).getCollection(remoteTableName.getCollectionName());
    }

    public List<MongoIndex> getIndexes(String schemaName, String tableName)
    {
        if (isView(schemaName, tableName)) {
            return ImmutableList.of();
        }
        MongoCollection<Document> collection = client.getDatabase(schemaName).getCollection(tableName);
        return MongoIndex.parse(collection.listIndexes());
    }

    public long deleteDocuments(RemoteTableName remoteTableName, TupleDomain<ColumnHandle> constraint)
    {
        Document filter = buildQuery(constraint);
        log.debug("Delete documents: collection: %s, filter: %s", remoteTableName, filter);

        DeleteResult result = getCollection(remoteTableName).deleteMany(filter);
        return result.getDeletedCount();
    }

    public MongoCursor<Document> execute(MongoTableHandle tableHandle, List<MongoColumnHandle> columns)
    {
        Document output = new Document();
        for (MongoColumnHandle column : columns) {
            output.append(column.getName(), 1);
        }
        MongoCollection<Document> collection = getCollection(tableHandle.getRemoteTableName());
        Document filter = buildFilter(tableHandle);
        FindIterable<Document> iterable = collection.find(filter).projection(output).collation(SIMPLE_COLLATION);
        tableHandle.getLimit().ifPresent(iterable::limit);
        log.debug("Find documents: collection: %s, filter: %s, projection: %s", tableHandle.getSchemaTableName(), filter, output);

        if (cursorBatchSize != 0) {
            iterable.batchSize(cursorBatchSize);
        }

        return iterable.iterator();
    }

    static Document buildFilter(MongoTableHandle table)
    {
        // Use $and operator because Document.putAll method overwrites existing entries where the key already exists
        ImmutableList.Builder<Document> filter = ImmutableList.builder();
        table.getFilter().ifPresent(json -> filter.add(parseFilter(json)));
        filter.add(buildQuery(table.getConstraint()));
        return andPredicate(filter.build());
    }

    @VisibleForTesting
    static Document buildQuery(TupleDomain<ColumnHandle> tupleDomain)
    {
        Document query = new Document();
        if (tupleDomain.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                MongoColumnHandle column = (MongoColumnHandle) entry.getKey();
                Optional<Document> predicate = buildPredicate(column, entry.getValue());
                predicate.ifPresent(query::putAll);
            }
        }

        return query;
    }

    private static Optional<Document> buildPredicate(MongoColumnHandle column, Domain domain)
    {
        String name = column.getName();
        Type type = column.getType();
        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            return Optional.of(documentOf(name, isNullPredicate()));
        }
        if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            return Optional.of(documentOf(name, isNotNullPredicate()));
        }

        List<Object> singleValues = new ArrayList<>();
        List<Document> disjuncts = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                Optional<Object> translated = translateValue(range.getSingleValue(), type);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                singleValues.add(translated.get());
            }
            else {
                Document rangeConjuncts = new Document();
                if (!range.isLowUnbounded()) {
                    Optional<Object> translated = translateValue(range.getLowBoundedValue(), type);
                    if (translated.isEmpty()) {
                        return Optional.empty();
                    }
                    rangeConjuncts.put(range.isLowInclusive() ? GTE_OP : GT_OP, translated.get());
                }
                if (!range.isHighUnbounded()) {
                    Optional<Object> translated = translateValue(range.getHighBoundedValue(), type);
                    if (translated.isEmpty()) {
                        return Optional.empty();
                    }
                    rangeConjuncts.put(range.isHighInclusive() ? LTE_OP : LT_OP, translated.get());
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                verify(!rangeConjuncts.isEmpty());
                disjuncts.add(rangeConjuncts);
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(documentOf(EQ_OP, singleValues.get(0)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(documentOf(IN_OP, singleValues));
        }

        if (domain.isNullAllowed()) {
            disjuncts.add(isNullPredicate());
        }

        return Optional.of(orPredicate(disjuncts.stream()
                .map(disjunct -> new Document(name, disjunct))
                .collect(toImmutableList())));
    }

    private static Optional<Object> translateValue(Object trinoNativeValue, Type type)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");
        requireNonNull(type, "type is null");
        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue), "%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(), type);

        if (type == BOOLEAN) {
            return Optional.of(trinoNativeValue);
        }

        if (type == TINYINT) {
            return Optional.of((long) SignedBytes.checkedCast(((Long) trinoNativeValue)));
        }

        if (type == SMALLINT) {
            return Optional.of((long) Shorts.checkedCast(((Long) trinoNativeValue)));
        }

        if (type == IntegerType.INTEGER) {
            return Optional.of((long) toIntExact(((Long) trinoNativeValue)));
        }

        if (type == BIGINT) {
            return Optional.of(trinoNativeValue);
        }

        if (type instanceof ObjectIdType) {
            return Optional.of(new ObjectId(((Slice) trinoNativeValue).getBytes()));
        }

        if (type instanceof VarcharType) {
            return Optional.of(((Slice) trinoNativeValue).toStringUtf8());
        }

        if (type == DATE) {
            long days = (long) trinoNativeValue;
            return Optional.of(LocalDate.ofEpochDay(days));
        }

        if (type == TIME_MILLIS) {
            long picos = (long) trinoNativeValue;
            return Optional.of(LocalTime.ofNanoOfDay(roundDiv(picos, PICOSECONDS_PER_NANOSECOND)));
        }

        if (type == TIMESTAMP_MILLIS) {
            long epochMicros = (long) trinoNativeValue;
            long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
            return Optional.of(LocalDateTime.ofInstant(instant, UTC));
        }

        if (type == TIMESTAMP_TZ_MILLIS) {
            long millisUtc = unpackMillisUtc((long) trinoNativeValue);
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return Optional.of(LocalDateTime.ofInstant(instant, UTC));
        }

        return Optional.empty();
    }

    private static Document documentOf(String key, Object value)
    {
        return new Document(key, value);
    }

    private static Document orPredicate(List<Document> values)
    {
        checkState(!values.isEmpty());
        if (values.size() == 1) {
            return values.get(0);
        }
        return new Document(OR_OP, values);
    }

    private static Document andPredicate(List<Document> values)
    {
        checkState(!values.isEmpty());
        if (values.size() == 1) {
            return values.get(0);
        }
        return new Document(AND_OP, values);
    }

    private static Document isNullPredicate()
    {
        return documentOf(EQ_OP, null);
    }

    private static Document isNotNullPredicate()
    {
        return documentOf(NOT_EQ_OP, null);
    }

    // Internal Schema management
    private Document getTableMetadata(String schemaName, String tableName)
            throws TableNotFoundException
    {
        MongoDatabase db = client.getDatabase(schemaName);
        MongoCollection<Document> schema = db.getCollection(schemaCollection);

        Document doc = schema
                .find(new Document(TABLE_NAME_KEY, tableName)).first();

        if (doc == null) {
            if (!collectionExists(db, tableName)) {
                throw new TableNotFoundException(new SchemaTableName(schemaName, tableName), format("Table '%s.%s' not found", schemaName, tableName), null);
            }
            Document metadata = new Document(TABLE_NAME_KEY, tableName);
            metadata.append(FIELDS_KEY, guessTableFields(schemaName, tableName));
            if (!indexExists(schema)) {
                schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
            }

            schema.insertOne(metadata);

            return metadata;
        }

        return doc;
    }

    public boolean collectionExists(MongoDatabase db, String collectionName)
    {
        for (String name : listCollectionNames(db.getName())) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    private boolean indexExists(MongoCollection<Document> schemaCollection)
    {
        return MongoIndex.parse(schemaCollection.listIndexes()).stream()
                .anyMatch(index -> index.getKeys().size() == 1 && TABLE_NAME_KEY.equals(index.getKeys().get(0).getName()));
    }

    private Set<String> getTableMetadataNames(String schemaName)
    {
        try (MongoCursor<Document> cursor = client.getDatabase(schemaName).getCollection(schemaCollection)
                .find().projection(new Document(TABLE_NAME_KEY, true)).iterator()) {
            return Streams.stream(cursor)
                    .map(document -> document.getString(TABLE_NAME_KEY))
                    .collect(toImmutableSet());
        }
    }

    private void createTableMetadata(RemoteTableName remoteSchemaTableName, List<MongoColumnHandle> columns, Optional<String> tableComment)
    {
        String remoteSchemaName = remoteSchemaTableName.getDatabaseName();
        String remoteTableName = remoteSchemaTableName.getCollectionName();

        MongoDatabase db = client.getDatabase(remoteSchemaName);
        Document metadata = new Document(TABLE_NAME_KEY, remoteTableName);

        ArrayList<Document> fields = new ArrayList<>();
        if (!columns.stream().anyMatch(c -> c.getName().equals("_id"))) {
            fields.add(new MongoColumnHandle("_id", OBJECT_ID, true, Optional.empty()).getDocument());
        }

        fields.addAll(columns.stream()
                .map(MongoColumnHandle::getDocument)
                .collect(toList()));

        metadata.append(FIELDS_KEY, fields);
        tableComment.ifPresent(comment -> metadata.append(COMMENT_KEY, comment));

        MongoCollection<Document> schema = db.getCollection(schemaCollection);
        if (!indexExists(schema)) {
            schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
        }

        schema.insertOne(metadata);
    }

    private boolean deleteTableMetadata(RemoteTableName remoteTableName)
    {
        MongoDatabase db = client.getDatabase(remoteTableName.getDatabaseName());
        if (!collectionExists(db, remoteTableName.getCollectionName()) &&
                db.getCollection(schemaCollection).find(new Document(TABLE_NAME_KEY, remoteTableName.getCollectionName())).first().isEmpty()) {
            return false;
        }

        DeleteResult result = db.getCollection(schemaCollection)
                .deleteOne(new Document(TABLE_NAME_KEY, remoteTableName.getCollectionName()));

        return result.getDeletedCount() == 1;
    }

    private List<Document> guessTableFields(String schemaName, String tableName)
    {
        MongoDatabase db = client.getDatabase(schemaName);
        Document doc = db.getCollection(tableName).find().first();
        if (doc == null) {
            // no records at the collection
            return ImmutableList.of();
        }

        ImmutableList.Builder<Document> builder = ImmutableList.builder();

        for (String key : doc.keySet()) {
            Object value = doc.get(key);
            Optional<TypeSignature> fieldType = guessFieldType(value);
            if (fieldType.isPresent()) {
                Document metadata = new Document();
                metadata.append(FIELDS_NAME_KEY, key);
                metadata.append(FIELDS_TYPE_KEY, fieldType.get().toString());
                metadata.append(FIELDS_HIDDEN_KEY,
                        key.equals("_id") && fieldType.get().equals(OBJECT_ID.getTypeSignature()));

                builder.add(metadata);
            }
            else {
                log.debug("Unable to guess field type from %s : %s", value == null ? "null" : value.getClass().getName(), value);
            }
        }

        return builder.build();
    }

    private Optional<TypeSignature> guessFieldType(Object value)
    {
        if (value == null) {
            return Optional.empty();
        }

        TypeSignature typeSignature = null;
        if (value instanceof String) {
            typeSignature = createUnboundedVarcharType().getTypeSignature();
        }
        if (value instanceof Binary) {
            typeSignature = VARBINARY.getTypeSignature();
        }
        else if (value instanceof Integer || value instanceof Long) {
            typeSignature = BIGINT.getTypeSignature();
        }
        else if (value instanceof Boolean) {
            typeSignature = BOOLEAN.getTypeSignature();
        }
        else if (value instanceof Float || value instanceof Double) {
            typeSignature = DOUBLE.getTypeSignature();
        }
        else if (value instanceof Date) {
            typeSignature = TIMESTAMP_MILLIS.getTypeSignature();
        }
        else if (value instanceof ObjectId) {
            typeSignature = OBJECT_ID.getTypeSignature();
        }
        else if (value instanceof List) {
            List<Optional<TypeSignature>> subTypes = ((List<?>) value).stream()
                    .map(this::guessFieldType)
                    .collect(toList());

            if (subTypes.isEmpty() || subTypes.stream().anyMatch(Optional::isEmpty)) {
                return Optional.empty();
            }

            Set<TypeSignature> signatures = subTypes.stream().map(Optional::get).collect(toSet());
            if (signatures.size() == 1) {
                typeSignature = new TypeSignature(StandardTypes.ARRAY, signatures.stream()
                        .map(TypeSignatureParameter::typeParameter)
                        .collect(Collectors.toList()));
            }
            else {
                // TODO: client doesn't handle empty field name row type yet
                typeSignature = new TypeSignature(StandardTypes.ROW,
                        IntStream.range(0, subTypes.size())
                                .mapToObj(idx -> TypeSignatureParameter.namedTypeParameter(
                                        new NamedTypeSignature(Optional.of(new RowFieldName(format("%s%d", implicitPrefix, idx + 1))), subTypes.get(idx).get())))
                                .collect(toList()));
            }
        }
        else if (value instanceof Document) {
            List<TypeSignatureParameter> parameters = new ArrayList<>();

            for (String key : ((Document) value).keySet()) {
                Optional<TypeSignature> fieldType = guessFieldType(((Document) value).get(key));
                if (fieldType.isPresent()) {
                    parameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(key)), fieldType.get())));
                }
            }
            if (!parameters.isEmpty()) {
                typeSignature = new TypeSignature(StandardTypes.ROW, parameters);
            }
        }
        else if (value instanceof DBRef) {
            List<TypeSignatureParameter> parameters = new ArrayList<>();

            TypeSignature idFieldType = guessFieldType(((DBRef) value).getId())
                    .orElseThrow(() -> new UnsupportedOperationException("Unable to guess $id field type of DBRef from: " + ((DBRef) value).getId()));

            parameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(DATABASE_NAME)), VARCHAR.getTypeSignature())));
            parameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(COLLECTION_NAME)), VARCHAR.getTypeSignature())));
            parameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(ID)), idFieldType)));

            typeSignature = new TypeSignature(StandardTypes.ROW, parameters);
        }

        return Optional.ofNullable(typeSignature);
    }

    public RemoteTableName toRemoteSchemaTableName(SchemaTableName schemaTableName)
    {
        String remoteSchemaName = toRemoteSchemaName(schemaTableName.getSchemaName());
        String remoteTableName = toRemoteTableName(remoteSchemaName, schemaTableName.getTableName());
        return new RemoteTableName(remoteSchemaName, remoteTableName);
    }

    private String toRemoteSchemaName(String schemaName)
    {
        verify(schemaName.equals(schemaName.toLowerCase(ENGLISH)), "schemaName not in lower-case: %s", schemaName);
        if (!caseInsensitiveNameMatching) {
            return schemaName;
        }
        if (SYSTEM_DATABASES.contains(schemaName)) {
            return schemaName;
        }
        for (String remoteSchemaName : listDatabaseNames()) {
            if (schemaName.equals(remoteSchemaName.toLowerCase(ENGLISH))) {
                return remoteSchemaName;
            }
        }
        return schemaName;
    }

    private MongoIterable<String> listDatabaseNames()
    {
        return client.listDatabases()
                .nameOnly(true)
                .authorizedDatabasesOnly(true)
                .map(result -> result.getString("name"));
    }

    private String toRemoteTableName(String schemaName, String tableName)
    {
        verify(tableName.equals(tableName.toLowerCase(ENGLISH)), "tableName not in lower-case: %s", tableName);
        if (!caseInsensitiveNameMatching) {
            return tableName;
        }
        for (String remoteTableName : listCollectionNames(schemaName)) {
            if (tableName.equals(remoteTableName.toLowerCase(ENGLISH))) {
                return remoteTableName;
            }
        }
        return tableName;
    }

    private List<String> listCollectionNames(String databaseName)
    {
        MongoDatabase database = client.getDatabase(databaseName);
        Document cursor = database.runCommand(new Document(AUTHORIZED_LIST_COLLECTIONS_COMMAND)).get("cursor", Document.class);

        List<Document> firstBatch = cursor.get("firstBatch", List.class);
        return firstBatch.stream()
                .map(document -> document.getString("name"))
                .collect(toImmutableList());
    }

    private boolean isView(String schemaName, String tableName)
    {
        Document listCollectionsCommand = new Document(ImmutableMap.<String, Object>builder()
                .put("listCollections", 1.0)
                .put("filter", documentOf("name", tableName))
                .put("nameOnly", true)
                .put("authorizedCollections", true)
                .buildOrThrow());
        Document cursor = client.getDatabase(schemaName).runCommand(listCollectionsCommand).get("cursor", Document.class);
        List<Document> firstBatch = cursor.get("firstBatch", List.class);
        if (firstBatch.isEmpty()) {
            return false;
        }
        String type = firstBatch.get(0).getString("type");
        return "view".equals(type);
    }
}
