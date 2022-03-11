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
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
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
import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.spi.HostAddress.fromParts;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MongoSession
{
    private static final Logger log = Logger.get(MongoSession.class);
    private static final List<String> SYSTEM_TABLES = Arrays.asList("system.indexes", "system.users", "system.version");

    private static final String TABLE_NAME_KEY = "table";
    private static final String COMMENT_KEY = "comment";
    private static final String FIELDS_KEY = "fields";
    private static final String FIELDS_NAME_KEY = "name";
    private static final String FIELDS_TYPE_KEY = "type";
    private static final String FIELDS_HIDDEN_KEY = "hidden";

    private static final String OR_OP = "$or";

    private static final String EQ_OP = "$eq";
    private static final String NOT_EQ_OP = "$ne";
    private static final String GTE_OP = "$gte";
    private static final String GT_OP = "$gt";
    private static final String LT_OP = "$lt";
    private static final String LTE_OP = "$lte";
    private static final String IN_OP = "$in";

    private static final String DATABASE_NAME = "databaseName";
    private static final String COLLECTION_NAME = "collectionName";
    private static final String ID = "id";

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
        return ImmutableList.copyOf(client.listDatabaseNames()).stream()
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    public void createSchema(String schemaName)
    {
        // Put an empty schema collection because MongoDB doesn't support a database without collections
        client.getDatabase(schemaName).createCollection(schemaCollection);
    }

    public void dropSchema(String schemaName)
    {
        client.getDatabase(schemaName).drop();
    }

    public Set<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        String schemaName = toRemoteSchemaName(schema);
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();

        builder.addAll(ImmutableList.copyOf(client.getDatabase(schemaName).listCollectionNames()).stream()
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

    public void createTable(SchemaTableName name, List<MongoColumnHandle> columns, Optional<String> comment)
    {
        createTableMetadata(name, columns, comment);
        // collection is created implicitly
    }

    public void dropTable(SchemaTableName tableName)
    {
        deleteTableMetadata(tableName);
        getCollection(tableName).drop();

        tableCache.invalidate(tableName);
    }

    public void setTableComment(SchemaTableName schemaTableName, Optional<String> comment)
    {
        String schemaName = toRemoteSchemaName(schemaTableName.getSchemaName());
        String tableName = toRemoteTableName(schemaName, schemaTableName.getTableName());

        Document metadata = getTableMetadata(schemaName, tableName);
        metadata.append(COMMENT_KEY, comment.orElse(null));

        client.getDatabase(schemaName).getCollection(schemaCollection)
                .findOneAndReplace(new Document(TABLE_NAME_KEY, tableName), metadata);

        tableCache.invalidate(schemaTableName);
    }

    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata columnMetadata)
    {
        String schemaName = toRemoteSchemaName(schemaTableName.getSchemaName());
        String tableName = toRemoteTableName(schemaName, schemaTableName.getTableName());

        Document metadata = getTableMetadata(schemaName, tableName);

        List<Document> columns = new ArrayList<>(getColumnMetadata(metadata));

        Document newColumn = new Document();
        newColumn.append(FIELDS_NAME_KEY, columnMetadata.getName());
        newColumn.append(FIELDS_TYPE_KEY, columnMetadata.getType().getTypeSignature().toString());
        newColumn.append(FIELDS_HIDDEN_KEY, false);
        columns.add(newColumn);

        metadata.append(FIELDS_KEY, columns);

        MongoDatabase db = client.getDatabase(schemaName);
        MongoCollection<Document> schema = db.getCollection(schemaCollection);
        schema.findOneAndReplace(new Document(TABLE_NAME_KEY, tableName), metadata);

        tableCache.invalidate(schemaTableName);
    }

    public void dropColumn(SchemaTableName schemaTableName, String columnName)
    {
        String remoteSchemaName = toRemoteSchemaName(schemaTableName.getSchemaName());
        String remoteTableName = toRemoteTableName(remoteSchemaName, schemaTableName.getTableName());

        Document metadata = getTableMetadata(remoteSchemaName, remoteTableName);

        List<Document> columns = getColumnMetadata(metadata).stream()
                .filter(document -> !document.getString(FIELDS_NAME_KEY).equals(columnName))
                .collect(toImmutableList());

        metadata.append(FIELDS_KEY, columns);

        MongoDatabase database = client.getDatabase(remoteSchemaName);
        MongoCollection<Document> schema = database.getCollection(schemaCollection);
        schema.findOneAndReplace(new Document(TABLE_NAME_KEY, remoteTableName), metadata);

        tableCache.invalidate(schemaTableName);
    }

    private MongoTable loadTableSchema(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        String schemaName = toRemoteSchemaName(schemaTableName.getSchemaName());
        String tableName = toRemoteTableName(schemaName, schemaTableName.getTableName());

        Document tableMeta = getTableMetadata(schemaName, tableName);

        ImmutableList.Builder<MongoColumnHandle> columnHandles = ImmutableList.builder();

        for (Document columnMetadata : getColumnMetadata(tableMeta)) {
            MongoColumnHandle columnHandle = buildColumnHandle(columnMetadata);
            columnHandles.add(columnHandle);
        }

        MongoTableHandle tableHandle = new MongoTableHandle(schemaTableName);
        return new MongoTable(tableHandle, columnHandles.build(), getIndexes(schemaName, tableName), getComment(tableMeta));
    }

    private MongoColumnHandle buildColumnHandle(Document columnMeta)
    {
        String name = columnMeta.getString(FIELDS_NAME_KEY);
        String typeString = columnMeta.getString(FIELDS_TYPE_KEY);
        boolean hidden = columnMeta.getBoolean(FIELDS_HIDDEN_KEY, false);

        Type type = typeManager.fromSqlType(typeString);

        return new MongoColumnHandle(name, type, hidden);
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

    public MongoCollection<Document> getCollection(SchemaTableName tableName)
    {
        return getCollection(tableName.getSchemaName(), tableName.getTableName());
    }

    private MongoCollection<Document> getCollection(String schema, String table)
    {
        String schemaName = toRemoteSchemaName(schema);
        String tableName = toRemoteTableName(schemaName, table);
        return client.getDatabase(schemaName).getCollection(tableName);
    }

    public List<MongoIndex> getIndexes(String schemaName, String tableName)
    {
        if (isView(schemaName, tableName)) {
            return ImmutableList.of();
        }
        MongoCollection<Document> collection = client.getDatabase(schemaName).getCollection(tableName);
        return MongoIndex.parse(collection.listIndexes());
    }

    public MongoCursor<Document> execute(MongoTableHandle tableHandle, List<MongoColumnHandle> columns)
    {
        Document output = new Document();
        for (MongoColumnHandle column : columns) {
            output.append(column.getName(), 1);
        }
        MongoCollection<Document> collection = getCollection(tableHandle.getSchemaTableName());
        Document query = buildQuery(tableHandle.getConstraint());
        FindIterable<Document> iterable = collection.find(query).projection(output);
        tableHandle.getLimit().ifPresent(iterable::limit);
        log.debug("Find documents: collection: %s, filter: %s, projection: %s", tableHandle.getSchemaTableName(), query.toJson(), output.toJson());

        if (cursorBatchSize != 0) {
            iterable.batchSize(cursorBatchSize);
        }

        return iterable.iterator();
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
            else {
                Document metadata = new Document(TABLE_NAME_KEY, tableName);
                metadata.append(FIELDS_KEY, guessTableFields(schemaName, tableName));
                if (!indexExists(schema)) {
                    schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
                }

                schema.insertOne(metadata);

                return metadata;
            }
        }

        return doc;
    }

    public boolean collectionExists(MongoDatabase db, String collectionName)
    {
        for (String name : db.listCollectionNames()) {
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
            throws TableNotFoundException
    {
        MongoDatabase db = client.getDatabase(schemaName);
        MongoCursor<Document> cursor = db.getCollection(schemaCollection)
                .find().projection(new Document(TABLE_NAME_KEY, true)).iterator();

        HashSet<String> names = new HashSet<>();
        while (cursor.hasNext()) {
            names.add((cursor.next()).getString(TABLE_NAME_KEY));
        }

        return names;
    }

    private void createTableMetadata(SchemaTableName schemaTableName, List<MongoColumnHandle> columns, Optional<String> tableComment)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        MongoDatabase db = client.getDatabase(schemaName);
        Document metadata = new Document(TABLE_NAME_KEY, tableName);

        ArrayList<Document> fields = new ArrayList<>();
        if (!columns.stream().anyMatch(c -> c.getName().equals("_id"))) {
            fields.add(new MongoColumnHandle("_id", OBJECT_ID, true).getDocument());
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

    private boolean deleteTableMetadata(SchemaTableName schemaTableName)
    {
        String schemaName = toRemoteSchemaName(schemaTableName.getSchemaName());
        String tableName = toRemoteTableName(schemaName, schemaTableName.getTableName());

        MongoDatabase db = client.getDatabase(schemaName);
        if (!collectionExists(db, tableName) &&
                db.getCollection(schemaCollection).find(new Document(TABLE_NAME_KEY, tableName)).first().isEmpty()) {
            return false;
        }

        DeleteResult result = db.getCollection(schemaCollection)
                .deleteOne(new Document(TABLE_NAME_KEY, tableName));

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

    private String toRemoteSchemaName(String schemaName)
    {
        verify(schemaName.equals(schemaName.toLowerCase(ENGLISH)), "schemaName not in lower-case: %s", schemaName);
        if (!caseInsensitiveNameMatching) {
            return schemaName;
        }
        for (String remoteSchemaName : client.listDatabaseNames()) {
            if (schemaName.equals(remoteSchemaName.toLowerCase(ENGLISH))) {
                return remoteSchemaName;
            }
        }
        return schemaName;
    }

    private String toRemoteTableName(String schemaName, String tableName)
    {
        verify(tableName.equals(tableName.toLowerCase(ENGLISH)), "tableName not in lower-case: %s", tableName);
        if (!caseInsensitiveNameMatching) {
            return tableName;
        }
        for (String remoteTableName : client.getDatabase(schemaName).listCollectionNames()) {
            if (tableName.equals(remoteTableName.toLowerCase(ENGLISH))) {
                return remoteTableName;
            }
        }
        return tableName;
    }

    private boolean isView(String schemaName, String tableName)
    {
        Document listCollectionsCommand = new Document(ImmutableMap.<String, Object>builder()
                .put("listCollections", 1.0)
                .put("filter", documentOf("name", tableName))
                .put("nameOnly", true)
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
