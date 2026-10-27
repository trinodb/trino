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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.decoder.protobuf.ProtoFileDescriptorBuilder;
import io.trino.spi.TrinoException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_REGISTRY_ERROR;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * A protobuf schema as stored in a Confluent schema registry: the {@code .proto} text plus
 * references to the subjects it imports. Compiling the text, which requires fetching the
 * referenced schemas, happens on first use and is retried on failure, so a registry outage
 * never turns into a cached broken schema.
 */
public final class ConfluentProtobufSchema
        implements ParsedSchema
{
    public static final String TYPE = "PROTOBUF";

    private static final String ROOT_FILE_NAME = "schema.proto";

    // Confluent serializers do not register references for the files of Confluent's own types,
    // so their imports resolve to these declarations unless the schema references a file of the
    // same name. The option extensions of meta.proto are left out, as options do not affect decoding.
    private static final Map<String, String> CONFLUENT_TYPES = ImmutableMap.of(
            "confluent/meta.proto",
            """
            syntax = "proto3";
            package confluent;
            message Meta {
              string doc = 1;
              map<string, string> params = 2;
              repeated string tags = 3;
            }
            """,
            "confluent/type/decimal.proto",
            """
            syntax = "proto3";
            package confluent.type;
            message Decimal {
              bytes value = 1;
              uint32 precision = 2;
              int32 scale = 3;
            }
            """);

    private final String schema;
    private final List<SchemaReference> references;
    // version, metadata and rule set are optional in the registry's schema entity
    private final Integer version;
    private final Metadata metadata;
    private final RuleSet ruleSet;
    // null only for schemas that are not attached to a registry client; such schemas cannot have references
    private final SchemaVersionFetcher schemaVersionFetcher;

    private FileDescriptor fileDescriptor;

    public ConfluentProtobufSchema(String schema, List<SchemaReference> references, SchemaVersionFetcher schemaVersionFetcher)
    {
        this(schema, references, null, null, null, schemaVersionFetcher);
    }

    public ConfluentProtobufSchema(
            String schema,
            List<SchemaReference> references,
            Integer version,
            Metadata metadata,
            RuleSet ruleSet,
            SchemaVersionFetcher schemaVersionFetcher)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.references = ImmutableList.copyOf(requireNonNull(references, "references is null"));
        this.version = version;
        this.metadata = metadata;
        this.ruleSet = ruleSet;
        this.schemaVersionFetcher = schemaVersionFetcher;
    }

    /**
     * The compiled schema: its own file, linked against the files of its references.
     *
     * @throws TrinoException if a reference cannot be fetched or does not exist, the references form a cycle, or the schema does not compile
     */
    public synchronized FileDescriptor getFileDescriptor()
    {
        if (fileDescriptor == null) {
            Map<String, String> referencedSources = resolveReferences();
            String rootFileName = ROOT_FILE_NAME;
            while (referencedSources.containsKey(rootFileName)) {
                rootFileName = "_" + rootFileName;
            }
            fileDescriptor = ProtoFileDescriptorBuilder.buildFileDescriptor(
                    rootFileName,
                    schema,
                    importPath -> Optional.ofNullable(referencedSources.get(importPath))
                            .or(() -> Optional.ofNullable(CONFLUENT_TYPES.get(importPath))));
        }
        return fileDescriptor;
    }

    /**
     * The first message declared in the schema, which is the message of a table whose subject does not name one.
     */
    public Descriptor getMessageDescriptor()
    {
        List<Descriptor> messages = getFileDescriptor().getMessageTypes();
        if (messages.isEmpty()) {
            throw new TrinoException(KAFKA_SCHEMA_ERROR, "Protobuf schema does not declare any message");
        }
        return messages.getFirst();
    }

    /**
     * The message a subject's table maps to. The {@code RecordNameStrategy} names a subject after the
     * message it holds ({@code <message full name>}) and the {@code TopicRecordNameStrategy} after the
     * topic and the message ({@code <topic>-<message full name>}); such a subject maps to that message,
     * which may be nested in another one. Any other subject, such as one of the {@code TopicNameStrategy},
     * maps to the first message declared in the schema.
     */
    public Descriptor getMessageDescriptor(String subject)
    {
        requireNonNull(subject, "subject is null");
        return messages()
                .filter(message -> subject.equals(message.getFullName()) || subject.endsWith("-" + message.getFullName()))
                .findFirst()
                .orElseGet(this::getMessageDescriptor);
    }

    /**
     * The message with the given full name, if the schema declares it.
     */
    public Optional<Descriptor> findMessage(String fullName)
    {
        requireNonNull(fullName, "fullName is null");
        return messages()
                .filter(message -> message.getFullName().equals(fullName))
                .findFirst();
    }

    private Stream<Descriptor> messages()
    {
        return getFileDescriptor().getMessageTypes().stream()
                .flatMap(ConfluentProtobufSchema::withNestedMessages);
    }

    private static Stream<Descriptor> withNestedMessages(Descriptor message)
    {
        // the entry type of a map field is not a message records are written with
        if (message.getOptions().getMapEntry()) {
            return Stream.empty();
        }
        return Stream.concat(Stream.of(message), message.getNestedTypes().stream().flatMap(ConfluentProtobufSchema::withNestedMessages));
    }

    private Map<String, String> resolveReferences()
    {
        Map<String, String> sources = new HashMap<>();
        Map<String, String> origins = new HashMap<>();
        collectReferences(references, sources, origins, new ArrayDeque<>());
        return sources;
    }

    private void collectReferences(List<SchemaReference> references, Map<String, String> sources, Map<String, String> origins, Deque<String> resolving)
    {
        for (SchemaReference reference : references) {
            if (reference.getName() == null || reference.getSubject() == null || reference.getVersion() == null) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Protobuf schema has an incomplete reference: %s", reference));
            }
            String name = reference.getName();
            String origin = reference.getSubject() + ":" + reference.getVersion();
            String existingOrigin = origins.get(name);
            if (existingOrigin != null) {
                if (!existingOrigin.equals(origin)) {
                    throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Protobuf schema reference '%s' points to both %s and %s", name, existingOrigin, origin));
                }
                continue;
            }
            if (resolving.contains(origin)) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Protobuf schema references form a cycle: %s -> %s", String.join(" -> ", resolving.reversed()), origin));
            }
            Schema referencedSchema = fetchReference(reference);
            String schemaType = Optional.ofNullable(referencedSchema.getSchemaType()).orElse("AVRO");
            if (!TYPE.equals(schemaType)) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Protobuf schema reference '%s' points to %s, which is a %s schema", name, origin, schemaType));
            }
            resolving.push(origin);
            try {
                collectReferences(Optional.ofNullable(referencedSchema.getReferences()).orElse(ImmutableList.of()), sources, origins, resolving);
            }
            finally {
                resolving.pop();
            }
            sources.put(name, referencedSchema.getSchema());
            origins.put(name, origin);
        }
    }

    private Schema fetchReference(SchemaReference reference)
    {
        checkState(schemaVersionFetcher != null, "schemaVersionFetcher is null");
        String origin = reference.getSubject() + ":" + reference.getVersion();
        Schema referencedSchema;
        try {
            referencedSchema = schemaVersionFetcher.getByVersion(reference.getSubject(), reference.getVersion(), false);
        }
        catch (RuntimeException e) {
            if (e.getCause() instanceof RestClientException restClientException && restClientException.getStatus() == HTTP_NOT_FOUND) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Protobuf schema reference '%s' points to %s, which does not exist in the schema registry", reference.getName(), origin), e);
            }
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to fetch protobuf schema reference '%s' (%s) from the schema registry: %s", reference.getName(), origin, e.getMessage()), e);
        }
        if (referencedSchema == null || referencedSchema.getSchema() == null) {
            throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Protobuf schema reference '%s' points to %s, which does not exist in the schema registry", reference.getName(), origin));
        }
        return referencedSchema;
    }

    @Override
    public String schemaType()
    {
        return TYPE;
    }

    @Override
    public String name()
    {
        List<Descriptor> messages = getFileDescriptor().getMessageTypes();
        if (messages.isEmpty()) {
            return null;
        }
        return messages.getFirst().getFullName();
    }

    @Override
    public String canonicalString()
    {
        return schema;
    }

    @Override
    public Integer version()
    {
        return version;
    }

    @Override
    public List<SchemaReference> references()
    {
        return references;
    }

    @Override
    public Metadata metadata()
    {
        return metadata;
    }

    @Override
    public RuleSet ruleSet()
    {
        return ruleSet;
    }

    @Override
    public ParsedSchema copy()
    {
        return new ConfluentProtobufSchema(schema, references, version, metadata, ruleSet, schemaVersionFetcher);
    }

    @Override
    public ParsedSchema copy(Integer version)
    {
        return new ConfluentProtobufSchema(schema, references, version, metadata, ruleSet, schemaVersionFetcher);
    }

    @Override
    public ParsedSchema copy(Metadata metadata, RuleSet ruleSet)
    {
        return new ConfluentProtobufSchema(schema, references, version, metadata, ruleSet, schemaVersionFetcher);
    }

    @Override
    public ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd, Map<SchemaEntity, Set<String>> tagsToRemove)
    {
        throw new UnsupportedOperationException("Modifying schema tags is not supported");
    }

    @Override
    public List<String> isBackwardCompatible(ParsedSchema previousSchema)
    {
        throw new UnsupportedOperationException("Checking protobuf schema compatibility is not supported");
    }

    @Override
    public Object rawSchema()
    {
        return schema;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConfluentProtobufSchema that)) {
            return false;
        }
        return schema.equals(that.schema)
                && references.equals(that.references)
                && Objects.equals(metadata, that.metadata)
                && Objects.equals(ruleSet, that.ruleSet);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, references, metadata, ruleSet);
    }

    @Override
    public String toString()
    {
        return schema;
    }
}
