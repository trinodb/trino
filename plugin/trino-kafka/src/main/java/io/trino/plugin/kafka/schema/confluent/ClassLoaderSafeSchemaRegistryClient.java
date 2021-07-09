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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import org.apache.avro.Schema;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.classloader.ThreadContextClassLoader.withClassLoader;
import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeSchemaRegistryClient
        implements SchemaRegistryClient
{
    private final SchemaRegistryClient delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeSchemaRegistryClient(SchemaRegistryClient delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public Optional<ParsedSchema> parseSchema(String schemaType, String schemaString, List<SchemaReference> references)
    {
        return withClassLoader(classLoader, () -> delegate.parseSchema(schemaType, schemaString, references));
    }

    @Override
    public int register(String subject, Schema schema)
    {
        return withClassLoader(classLoader, () -> delegate.register(subject, schema));
    }

    @Override
    public int register(String subject, ParsedSchema parsedSchema)
    {
        return withClassLoader(classLoader, () -> delegate.register(subject, parsedSchema));
    }

    @Override
    public int register(String subject, Schema schema, int version, int id)
    {
        return withClassLoader(classLoader, () -> delegate.register(subject, schema, version, id));
    }

    @Override
    public int register(String subject, ParsedSchema parsedSchema, int version, int id)
    {
        return withClassLoader(classLoader, () -> delegate.register(subject, parsedSchema, version, id));
    }

    @Override
    public Schema getByID(int id)
    {
        return withClassLoader(classLoader, () -> delegate.getByID(id));
    }

    @Override
    public Schema getById(int id)
    {
        return withClassLoader(classLoader, () -> delegate.getById(id));
    }

    @Override
    public ParsedSchema getSchemaById(int id)
    {
        return withClassLoader(classLoader, () -> delegate.getSchemaById(id));
    }

    @Override
    public Schema getBySubjectAndID(String subject, int id)
    {
        return withClassLoader(classLoader, () -> delegate.getBySubjectAndID(subject, id));
    }

    @Override
    public Schema getBySubjectAndId(String subject, int id)
    {
        return withClassLoader(classLoader, () -> delegate.getBySubjectAndId(subject, id));
    }

    @Override
    public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
    {
        return withClassLoader(classLoader, () -> delegate.getSchemaBySubjectAndId(subject, id));
    }

    @Override
    public Collection<String> getAllSubjectsById(int id)
    {
        return withClassLoader(classLoader, () -> delegate.getAllSubjectsById(id));
    }

    @Override
    public Collection<SubjectVersion> getAllVersionsById(int id)
    {
        return withClassLoader(classLoader, () -> delegate.getAllVersionsById(id));
    }

    @Override
    public io.confluent.kafka.schemaregistry.client.rest.entities.Schema getByVersion(String subject, int version, boolean lookupDeletedSchema)
    {
        return withClassLoader(classLoader, () -> delegate.getByVersion(subject, version, lookupDeletedSchema));
    }

    @Override
    public SchemaMetadata getLatestSchemaMetadata(String subject)
    {
        return withClassLoader(classLoader, () -> delegate.getLatestSchemaMetadata(subject));
    }

    @Override
    public SchemaMetadata getSchemaMetadata(String subject, int version)
    {
        return withClassLoader(classLoader, () -> delegate.getSchemaMetadata(subject, version));
    }

    @Override
    public int getVersion(String subject, Schema schema)
    {
        return withClassLoader(classLoader, () -> delegate.getVersion(subject, schema));
    }

    @Override
    public int getVersion(String subject, ParsedSchema parsedSchema)
    {
        return withClassLoader(classLoader, () -> delegate.getVersion(subject, parsedSchema));
    }

    @Override
    public List<Integer> getAllVersions(String subject)
    {
        return withClassLoader(classLoader, () -> delegate.getAllVersions(subject));
    }

    @Override
    public boolean testCompatibility(String subject, Schema schema)
    {
        return withClassLoader(classLoader, () -> delegate.testCompatibility(subject, schema));
    }

    @Override
    public boolean testCompatibility(String subject, ParsedSchema parsedSchema)
    {
        return withClassLoader(classLoader, () -> delegate.testCompatibility(subject, parsedSchema));
    }

    @Override
    public String updateCompatibility(String subject, String compatibility)
    {
        return withClassLoader(classLoader, () -> delegate.updateCompatibility(subject, compatibility));
    }

    @Override
    public String getCompatibility(String subject)
    {
        return withClassLoader(classLoader, () -> delegate.getCompatibility(subject));
    }

    @Override
    public String setMode(String mode)
    {
        return withClassLoader(classLoader, () -> delegate.setMode(mode));
    }

    @Override
    public String setMode(String mode, String subject)
    {
        return withClassLoader(classLoader, () -> delegate.setMode(mode, subject));
    }

    @Override
    public String getMode()
    {
        return withClassLoader(classLoader, () -> delegate.getMode());
    }

    @Override
    public String getMode(String subject)
    {
        return withClassLoader(classLoader, () -> delegate.getMode(subject));
    }

    @Override
    public Collection<String> getAllSubjects()
    {
        return withClassLoader(classLoader, () -> delegate.getAllSubjects());
    }

    @Override
    public int getId(String subject, Schema schema)
    {
        return withClassLoader(classLoader, () -> delegate.getId(subject, schema));
    }

    @Override
    public int getId(String subject, ParsedSchema parsedSchema)
    {
        return withClassLoader(classLoader, () -> delegate.getId(subject, parsedSchema));
    }

    @Override
    public List<Integer> deleteSubject(String subject)
    {
        return withClassLoader(classLoader, () -> delegate.deleteSubject(subject));
    }

    @Override
    public List<Integer> deleteSubject(Map<String, String> requestProperties, String subject)
    {
        return withClassLoader(classLoader, () -> delegate.deleteSubject(subject));
    }

    @Override
    public Integer deleteSchemaVersion(String subject, String version)
    {
        return withClassLoader(classLoader, () -> delegate.deleteSchemaVersion(subject, version));
    }

    @Override
    public Integer deleteSchemaVersion(Map<String, String> requestProperties, String subject, String version)
    {
        return withClassLoader(classLoader, () -> delegate.deleteSchemaVersion(requestProperties, subject, version));
    }

    @Override
    public void reset()
    {
        withClassLoader(classLoader, delegate::reset);
    }
}
