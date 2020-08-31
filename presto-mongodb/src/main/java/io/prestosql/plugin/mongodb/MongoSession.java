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
package io.prestosql.plugin.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MongoSession
{
    private static final Logger log = Logger.get(MongoSession.class);
    private static final List<String> SYSTEM_TABLES = Arrays.asList("system.indexes", "system.users", "system.version");

    private static final String TABLE_NAME_KEY = "table";
    private static final String FIELDS_KEY = "fields";
    private static final String FIELDS_NAME_KEY = "name";
    private static final String FIELDS_TYPE_KEY = "type";
    private static final String FIELDS_HIDDEN_KEY = "hidden";

    private static final String OR_OP = "$or";
    private static final String AND_OP = "$and";
    private static final String NOT_OP = "$not";
    private static final String NOR_OP = "$nor";

    private static final String EQ_OP = "$eq";
    private static final String NOT_EQ_OP = "$ne";
    private static final String EXISTS_OP = "$exists";
    private static final String GTE_OP = "$gte";
    private static final String GT_OP = "$gt";
    private static final String LT_OP = "$lt";
    private static final String LTE_OP = "$lte";
    private static final String IN_OP = "$in";
    private static final String NOTIN_OP = "$nin";

    private final TypeManager typeManager;
    private final MongoClient client;

    private final String schemaCollection;
    private final boolean caseInsensitiveNameMatching;
    private final int cursorBatchSize;

    private final LoadingCache<SchemaTableName, MongoTable> tableCache;
    private final String implicitPrefix;

    public MongoSession(TypeManager typeManager, MongoClient client, MongoClientConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.client = requireNonNull(client, "client is null");
        this.schemaCollection = requireNonNull(config.getSchemaCollection(), "config.getSchemaCollection() is null");
        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        this.cursorBatchSize = config.getCursorBatchSize();
        this.implicitPrefix = requireNonNull(config.getImplicitRowFieldPrefix(), "config.getImplicitRowFieldPrefix() is null");

        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, HOURS)  // TODO: Configure
                .refreshAfterWrite(1, MINUTES)
                .build(CacheLoader.from(this::loadTableSchema));
    }

    public void shutdown()
    {
        client.close();
    }

    public List<String> getAllSchemas()
    {
        return ImmutableList.copyOf(client.listDatabaseNames()).stream()
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
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
            return tableCache.getUnchecked(tableName);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    public void createTable(SchemaTableName name, List<MongoColumnHandle> columns)
    {
        createTableMetadata(name, columns);
        // collection is created implicitly
    }

    public void dropTable(SchemaTableName tableName)
    {
        deleteTableMetadata(tableName);
        getCollection(tableName).drop();

        tableCache.invalidate(tableName);
    }

    private MongoTable loadTableSchema(SchemaTableName tableName)
            throws TableNotFoundException
    {
        Document tableMeta = getTableMetadata(tableName);

        ImmutableList.Builder<MongoColumnHandle> columnHandles = ImmutableList.builder();

        for (Document columnMetadata : getColumnMetadata(tableMeta)) {
            MongoColumnHandle columnHandle = buildColumnHandle(columnMetadata);
            columnHandles.add(columnHandle);
        }

        MongoTableHandle tableHandle = new MongoTableHandle(tableName);
        return new MongoTable(tableHandle, columnHandles.build(), getIndexes(tableName));
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

    public List<MongoIndex> getIndexes(SchemaTableName tableName)
    {
        if (isView(tableName)) {
            return ImmutableList.of();
        }
        return MongoIndex.parse(getCollection(tableName).listIndexes());
    }

    public MongoCursor<Document> execute(MongoTableHandle tableHandle, List<MongoColumnHandle> columns)
    {
        Document output = new Document();
        for (MongoColumnHandle column : columns) {
            output.append(column.getName(), 1);
        }
        MongoCollection<Document> collection = getCollection(tableHandle.getSchemaTableName());
        FindIterable<Document> iterable = collection.find(buildQuery(tableHandle.getConstraint())).projection(output);

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
                if (!range.getLow().isLowerUnbounded()) {
                    Optional<Object> translated = translateValue(range.getLow().getValue(), type);
                    if (translated.isEmpty()) {
                        return Optional.empty();
                    }
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.put(GT_OP, translated.get());
                            break;
                        case EXACTLY:
                            rangeConjuncts.put(GTE_OP, translated.get());
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    Optional<Object> translated = translateValue(range.getHigh().getValue(), type);
                    if (translated.isEmpty()) {
                        return Optional.empty();
                    }
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.put(LTE_OP, translated.get());
                            break;
                        case BELOW:
                            rangeConjuncts.put(LT_OP, translated.get());
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
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

    private static Optional<Object> translateValue(Object prestoNativeValue, Type type)
    {
        requireNonNull(prestoNativeValue, "prestoNativeValue is null");
        requireNonNull(type, "type is null");
        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(prestoNativeValue), "%s (%s) is not a valid representation for %s", prestoNativeValue, prestoNativeValue.getClass(), type);

        if (type == TINYINT) {
            return Optional.of((long) SignedBytes.checkedCast(((Long) prestoNativeValue)));
        }

        if (type == SMALLINT) {
            return Optional.of((long) Shorts.checkedCast(((Long) prestoNativeValue)));
        }

        if (type == IntegerType.INTEGER) {
            return Optional.of((long) toIntExact(((Long) prestoNativeValue)));
        }

        if (type == BIGINT) {
            return Optional.of(prestoNativeValue);
        }

        if (type instanceof ObjectIdType) {
            return Optional.of(new ObjectId(((Slice) prestoNativeValue).getBytes()));
        }

        if (type instanceof VarcharType) {
            return Optional.of(((Slice) prestoNativeValue).toStringUtf8());
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
        return documentOf(EXISTS_OP, true).append(EQ_OP, null);
    }

    private static Document isNotNullPredicate()
    {
        return documentOf(NOT_EQ_OP, null);
    }

    // Internal Schema management
    private Document getTableMetadata(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        String schemaName = toRemoteSchemaName(schemaTableName.getSchemaName());
        String tableName = toRemoteTableName(schemaName, schemaTableName.getTableName());

        MongoDatabase db = client.getDatabase(schemaName);
        MongoCollection<Document> schema = db.getCollection(schemaCollection);

        Document doc = schema
                .find(new Document(TABLE_NAME_KEY, tableName)).first();

        if (doc == null) {
            if (!collectionExists(db, tableName)) {
                throw new TableNotFoundException(schemaTableName);
            }
            else {
                Document metadata = new Document(TABLE_NAME_KEY, tableName);
                metadata.append(FIELDS_KEY, guessTableFields(schemaName, tableName));

                schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
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

    private void createTableMetadata(SchemaTableName schemaTableName, List<MongoColumnHandle> columns)
            throws TableNotFoundException
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

        MongoCollection<Document> schema = db.getCollection(schemaCollection);
        schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
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
                // TODO: presto cli doesn't handle empty field name row type yet
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

    private boolean isView(SchemaTableName tableName)
    {
        Document listCollectionsCommand = new Document(new ImmutableMap.Builder<String, Object>()
                .put("listCollections", 1.0)
                .put("filter", documentOf("name", tableName.getTableName()))
                .put("nameOnly", true)
                .build());
        Document cursor = client.getDatabase(tableName.getSchemaName()).runCommand(listCollectionsCommand).get("cursor", Document.class);
        List<Document> firstBatch = cursor.get("firstBatch", List.class);
        if (firstBatch.isEmpty()) {
            return false;
        }
        String type = firstBatch.get(0).getString("type");
        return "view".equals(type);
    }
}
