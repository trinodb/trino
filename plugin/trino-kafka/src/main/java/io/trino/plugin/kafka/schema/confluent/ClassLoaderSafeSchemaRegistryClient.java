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
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.parseSchema(schemaType, schemaString, references);
        }
    }

    @Override
    public int register(String subject, Schema schema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.register(subject, schema);
        }
    }

    @Override
    public int register(String subject, ParsedSchema parsedSchema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.register(subject, parsedSchema);
        }
    }

    @Override
    public int register(String subject, Schema schema, int version, int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.register(subject, schema, version, id);
        }
    }

    @Override
    public int register(String subject, ParsedSchema parsedSchema, int version, int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.register(subject, parsedSchema, version, id);
        }
    }

    @Override
    public int register(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.register(subject, schema, normalize);
        }
    }

    @Override
    public Schema getByID(int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getByID(id);
        }
    }

    @Override
    public Schema getById(int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getById(id);
        }
    }

    @Override
    public int getId(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getId(subject, schema, normalize);
        }
    }

    @Override
    public Optional<ParsedSchema> parseSchema(io.confluent.kafka.schemaregistry.client.rest.entities.Schema schema)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.parseSchema(schema);
        }
    }

    @Override
    public List<ParsedSchema> getSchemas(String subjectPrefix, boolean lookupDeletedSchema, boolean latestOnly)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSchemas(subjectPrefix, lookupDeletedSchema, latestOnly);
        }
    }

    @Override
    public SchemaMetadata getSchemaMetadata(String subject, int version, boolean lookupDeletedSchema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSchemaMetadata(subject, version, lookupDeletedSchema);
        }
    }

    @Override
    public int getVersion(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getVersion(subject, schema, normalize);
        }
    }

    @Override
    public List<Integer> getAllVersions(String subject, boolean lookupDeletedSchema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAllVersions(subject, lookupDeletedSchema);
        }
    }

    @Override
    public List<String> testCompatibilityVerbose(String subject, ParsedSchema schema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.testCompatibilityVerbose(subject, schema);
        }
    }

    @Override
    public void deleteCompatibility(String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.deleteCompatibility(subject);
        }
    }

    @Override
    public void deleteMode(String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.deleteMode(subject);
        }
    }

    @Override
    public Collection<String> getAllSubjects(boolean lookupDeletedSubject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAllSubjects(lookupDeletedSubject);
        }
    }

    @Override
    public Collection<String> getAllSubjectsByPrefix(String subjectPrefix)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAllSubjectsByPrefix(subjectPrefix);
        }
    }

    @Override
    public List<Integer> deleteSubject(String subject, boolean isPermanent)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSubject(subject, isPermanent);
        }
    }

    @Override
    public List<Integer> deleteSubject(Map<String, String> requestProperties, String subject, boolean isPermanent)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSubject(requestProperties, subject, isPermanent);
        }
    }

    @Override
    public Integer deleteSchemaVersion(String subject, String version, boolean isPermanent)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSchemaVersion(subject, version, isPermanent);
        }
    }

    @Override
    public Integer deleteSchemaVersion(Map<String, String> requestProperties, String subject, String version, boolean isPermanent)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSchemaVersion(requestProperties, subject, version, isPermanent);
        }
    }

    @Override
    public ParsedSchema getSchemaById(int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSchemaById(id);
        }
    }

    @Override
    public Schema getBySubjectAndID(String subject, int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getBySubjectAndID(subject, id);
        }
    }

    @Override
    public Schema getBySubjectAndId(String subject, int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getBySubjectAndId(subject, id);
        }
    }

    @Override
    public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSchemaBySubjectAndId(subject, id);
        }
    }

    @Override
    public Collection<String> getAllSubjectsById(int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAllSubjectsById(id);
        }
    }

    @Override
    public Collection<SubjectVersion> getAllVersionsById(int id)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAllVersionsById(id);
        }
    }

    @Override
    public io.confluent.kafka.schemaregistry.client.rest.entities.Schema getByVersion(String subject, int version, boolean lookupDeletedSchema)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getByVersion(subject, version, lookupDeletedSchema);
        }
    }

    @Override
    public SchemaMetadata getLatestSchemaMetadata(String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getLatestSchemaMetadata(subject);
        }
    }

    @Override
    public SchemaMetadata getSchemaMetadata(String subject, int version)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSchemaMetadata(subject, version);
        }
    }

    @Override
    public int getVersion(String subject, Schema schema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getVersion(subject, schema);
        }
    }

    @Override
    public int getVersion(String subject, ParsedSchema parsedSchema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getVersion(subject, parsedSchema);
        }
    }

    @Override
    public List<Integer> getAllVersions(String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAllVersions(subject);
        }
    }

    @Override
    public boolean testCompatibility(String subject, Schema schema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.testCompatibility(subject, schema);
        }
    }

    @Override
    public boolean testCompatibility(String subject, ParsedSchema parsedSchema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.testCompatibility(subject, parsedSchema);
        }
    }

    @Override
    public String updateCompatibility(String subject, String compatibility)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.updateCompatibility(subject, compatibility);
        }
    }

    @Override
    public String getCompatibility(String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getCompatibility(subject);
        }
    }

    @Override
    public String setMode(String mode)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.setMode(mode);
        }
    }

    @Override
    public String setMode(String mode, String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.setMode(mode, subject);
        }
    }

    @Override
    public String getMode()
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMode();
        }
    }

    @Override
    public String getMode(String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMode(subject);
        }
    }

    @Override
    public Collection<String> getAllSubjects()
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getAllSubjects();
        }
    }

    @Override
    public int getId(String subject, Schema schema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getId(subject, schema);
        }
    }

    @Override
    public int getId(String subject, ParsedSchema parsedSchema)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getId(subject, parsedSchema);
        }
    }

    @Override
    public List<Integer> deleteSubject(String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSubject(subject);
        }
    }

    @Override
    public List<Integer> deleteSubject(Map<String, String> requestProperties, String subject)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSubject(subject);
        }
    }

    @Override
    public Integer deleteSchemaVersion(String subject, String version)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSchemaVersion(subject, version);
        }
    }

    @Override
    public Integer deleteSchemaVersion(Map<String, String> requestProperties, String subject, String version)
            throws IOException, RestClientException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.deleteSchemaVersion(requestProperties, subject, version);
        }
    }

    @Override
    public void reset()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.reset();
        }
    }
}
