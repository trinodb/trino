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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.base.Suppliers;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.Tasks;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.catalog.nessie.NessieIcebergUtil.buildCommitMeta;
import static io.trino.plugin.iceberg.catalog.nessie.NessieIcebergUtil.toKey;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NessieIcebergClient
{
    private final NessieApiV1 nessieApi;
    private final Supplier<UpdateableReference> reference;

    public NessieIcebergClient(NessieApiV1 nessieApi, String requestedRef, String requestedHash)
    {
        this.nessieApi = requireNonNull(nessieApi, "nessieApi is null");
        this.reference = Suppliers.memoize(() -> loadReference(Optional.ofNullable(requestedRef), requestedHash));
    }

    public UpdateableReference getReference()
    {
        return reference.get();
    }

    public List<SchemaTableName> listTables(Optional<String> namespace)
    {
        try {
            return nessieApi.getEntries().reference(getReference().getReference())
                    .get().getEntries().stream()
                    .filter(entry -> namespace.map(ns -> Namespace.parse(ns).equals(entry.getName().getNamespace())).orElse(true))
                    .filter(entry -> entry.getType() == Content.Type.ICEBERG_TABLE)
                    .map(entry -> new SchemaTableName(entry.getName().getNamespace().name(), entry.getName().getName()))
                    .collect(toImmutableList());
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Nullable
    public IcebergTable loadTable(SchemaTableName schemaTableName)
    {
        try {
            ContentKey key = NessieIcebergUtil.toKey(schemaTableName);
            Content table = nessieApi.getContent().key(key).reference(getReference().getReference())
                    .get().get(key);
            return table != null ? table.unwrap(IcebergTable.class).orElse(null) : null;
        }
        catch (NessieNotFoundException e) {
            return null;
        }
    }

    private UpdateableReference loadReference(Optional<String> requestedRef, String hash)
    {
        try {
            Reference ref = requestedRef.isEmpty() ? nessieApi.getDefaultBranch()
                    : nessieApi.getReference().refName(requestedRef.get()).get();
            // hash is currently always null, but a user will eventually be able to specify a particular branch/tag with a given hash to operate on
            if (hash != null) {
                if (ref instanceof Branch) {
                    ref = Branch.of(ref.getName(), hash);
                }
                else {
                    ref = Tag.of(ref.getName(), hash);
                }
            }
            return new UpdateableReference(ref, hash != null);
        }
        catch (NessieNotFoundException e) {
            if (requestedRef.isPresent()) {
                throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Nessie ref '%s' does not exist. This ref must exist before creating a NessieCatalog.", requestedRef), e);
            }

            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Nessie does not have an existing default branch." +
                    "Either configure an alternative ref via 'iceberg.nessie.ref' or create the default branch on the server.", e);
        }
    }

    public void refreshReference()
    {
        try {
            getReference().refresh();
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to refresh as ref '%s' is no longer valid.", getReference().getName()), e);
        }
    }

    public List<String> listNamespaces()
    {
        try {
            return nessieApi.getMultipleNamespaces()
                    .refName(getReference().getName())
                    .get().getNamespaces().stream()
                    .map(Namespace::name)
                    .collect(Collectors.toList());
        }
        catch (NessieReferenceNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Cannot list Namespaces: ref is no longer valid", e);
        }
    }

    public void createNamespace(String namespace, Map<String, Object> properties)
    {
        try {
            getReference().checkMutable();
            Map<String, String> props = new HashMap<>();
            properties.forEach((key, value) -> props.put(key, value.toString()));
            nessieApi.createNamespace()
                    .refName(getReference().getName())
                    .namespace(namespace)
                    .properties(props)
                    .create();
            refreshReference();
        }
        catch (NessieNamespaceAlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(namespace);
        }
        catch (NessieReferenceNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Cannot create Namespace '%s': ref is no longer valid", namespace), e);
        }
    }

    public void dropNamespace(String namespace)
    {
        try {
            getReference().checkMutable();
            nessieApi.deleteNamespace()
                    .refName(getReference().getName())
                    .namespace(namespace)
                    .delete();
            refreshReference();
        }
        catch (NessieNamespaceNotFoundException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (NessieReferenceNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Cannot drop Namespace '%s': ref is no longer valid", namespace), e);
        }
        catch (NessieNamespaceNotEmptyException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Namespace '%s' is not empty. One or more tables exist", namespace), e);
        }
    }

    public Map<String, Object> loadNamespaceMetadata(String namespace)
    {
        try {
            Map<String, String> properties = nessieApi.getNamespace()
                    .refName(getReference().getName())
                    .namespace(namespace)
                    .get()
                    .getProperties();
            return new HashMap<>(properties);
        }
        catch (NessieNamespaceNotFoundException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (NessieReferenceNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR,
                    format("Cannot load Namespace '%s': ref is no longer valid", namespace),
                    e);
        }
    }

    public void dropTable(SchemaTableName schemaTableName, String user)
    {
        getReference().checkMutable();

        IcebergTable existingTable = loadTable(schemaTableName);
        if (existingTable == null) {
            return;
        }

        CommitMultipleOperationsBuilder commitBuilderBase = nessieApi.commitMultipleOperations()
                .commitMeta(buildCommitMeta(user, format("Trino Iceberg delete table %s", schemaTableName)))
                .operation(Operation.Delete.of(toKey(schemaTableName)));

        // We try to drop the table. Simple retry after ref update.
        try {
            Tasks.foreach(commitBuilderBase)
                    .retry(5)
                    .stopRetryOn(NessieNotFoundException.class)
                    .throwFailureWhenFinished()
                    .onFailure((o, exception) -> refreshReference())
                    .run(commitBuilder -> {
                        Branch branch = commitBuilder
                                .branch(getReference().getAsBranch())
                                .commit();
                        getReference().updateReference(branch);
                    }, BaseNessieClientServerException.class);
        }
        catch (NessieConflictException e) {
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            throw new CommitFailedException(e,
                    "Cannot drop table '%s': failed after retry (update ref '%s' and retry)", schemaTableName, getReference().getName());
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR,
                    format("Cannot drop table '%s': ref '%s' no longer exists", schemaTableName, getReference().getName()), e);
        }
        catch (BaseNessieClientServerException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Cannot drop table '%s': unknown error", schemaTableName), e);
        }
    }

    public void renameTable(SchemaTableName from, SchemaTableName to, String user)
    {
        getReference().checkMutable();

        IcebergTable existingFromTable = loadTable(from);
        if (existingFromTable == null) {
            throw new SchemaNotFoundException("Table '%s' doesn't exists", from.getTableName());
        }
        IcebergTable existingToTable = loadTable(to);
        if (existingToTable != null) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Table '%s' already exists", to.getTableName()));
        }

        CommitMultipleOperationsBuilder operations = nessieApi.commitMultipleOperations()
                .commitMeta(buildCommitMeta(user, format("Trino Iceberg rename table from '%s' to '%s'", from, to)))
                .operation(Operation.Put.of(toKey(to), existingFromTable,
                        existingFromTable))
                .operation(Operation.Delete.of(toKey(from)));

        try {
            Tasks.foreach(operations)
                    .retry(5)
                    .stopRetryOn(NessieNotFoundException.class)
                    .throwFailureWhenFinished()
                    .onFailure((o, exception) -> refreshReference())
                    .run(ops -> {
                        Branch branch = ops
                                .branch(getReference().getAsBranch())
                                .commit();
                        getReference().updateReference(branch);
                    }, BaseNessieClientServerException.class);
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR,
                    format("Cannot rename table '%s' to '%s': ref '%s' no longer exists", from.getTableName(), to.getTableName(), getReference().getName()), e);
        }
        catch (BaseNessieClientServerException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR,
                    format("Cannot rename table '%s' to '%s': ref '%s' is not up to date", from.getTableName(), to.getTableName(), getReference().getName()), e);
        }
        catch (HttpClientException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    public void commitTable(TableMetadata metadata, SchemaTableName tableName, String metadataLocation, String user)
    {
        getReference().checkMutable();
        try {
            ImmutableIcebergTable.Builder newTableBuilder = ImmutableIcebergTable.builder();
            Snapshot snapshot = metadata.currentSnapshot();
            long snapshotId = snapshot != null ? snapshot.snapshotId() : -1L;
            IcebergTable newTable = newTableBuilder
                    .snapshotId(snapshotId)
                    .schemaId(metadata.currentSchemaId())
                    .specId(metadata.defaultSpecId())
                    .sortOrderId(metadata.defaultSortOrderId())
                    .metadataLocation(metadataLocation)
                    .build();

            Branch branch = nessieApi.commitMultipleOperations()
                    .operation(Operation.Put.of(NessieIcebergUtil.toKey(tableName), newTable))
                    .commitMeta(buildCommitMeta(user, format("Trino Iceberg add table %s", tableName)))
                    .branch(getReference().getAsBranch())
                    .commit();
            getReference().updateReference(branch);
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Cannot commit: ref '%s' no longer exists", getReference().getName()), e);
        }
        catch (NessieConflictException e) {
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            throw new CommitFailedException(e, "Cannot commit: ref hash is out of date. Update the ref '%s' and try again", getReference().getName());
        }
    }

    private class UpdateableReference
    {
        private Reference reference;
        private final boolean mutable;

        /**
         * Construct a new {@link UpdateableReference} using a Nessie reference object and a flag
         * whether an explicit hash was used to create the reference object.
         */
        public UpdateableReference(Reference reference, boolean hashReference)
        {
            this.reference = reference;
            this.mutable = reference instanceof Branch && !hashReference;
        }

        public void refresh()
                throws NessieNotFoundException
        {
            if (!mutable) {
                return;
            }
            reference = nessieApi.getReference().refName(reference.getName()).get();
        }

        public void updateReference(Reference ref)
        {
            checkState(mutable, "Hash references cannot be updated.");
            this.reference = requireNonNull(ref);
        }

        public boolean isBranch()
        {
            return reference instanceof Branch;
        }

        public String getHash()
        {
            return reference.getHash();
        }

        public Branch getAsBranch()
        {
            if (!isBranch()) {
                throw new IllegalArgumentException("Reference is not a branch");
            }
            return (Branch) reference;
        }

        public Reference getReference()
        {
            return reference;
        }

        public void checkMutable()
        {
            checkArgument(mutable, "You can only mutate tables when using a branch without a hash or timestamp.");
        }

        public String getName()
        {
            return reference.getName();
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", UpdateableReference.class.getSimpleName() + "[", "]")
                    .add("reference=" + reference)
                    .add("mutable=" + mutable)
                    .toString();
        }
    }
}
