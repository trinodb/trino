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
package io.trino.plugin.hive.metastore.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.Database;
import io.trino.spi.security.PrincipalType;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DatabaseMetadata
{
    private final Optional<String> writerVersion;
    private final Optional<String> ownerName;
    private final Optional<PrincipalType> ownerType;
    private final Map<String, String> parameters;

    @JsonCreator
    public DatabaseMetadata(
            @JsonProperty("writerVersion") Optional<String> writerVersion,
            @JsonProperty("ownerName") Optional<String> ownerName,
            @JsonProperty("ownerType") Optional<PrincipalType> ownerType,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.writerVersion = requireNonNull(writerVersion, "writerVersion is null");
        this.ownerName = requireNonNull(ownerName, "ownerName is null");
        this.ownerType = requireNonNull(ownerType, "ownerType is null");
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    public DatabaseMetadata(String currentVersion, Database database)
    {
        this.writerVersion = Optional.of(requireNonNull(currentVersion, "currentVersion is null"));
        this.ownerName = database.getOwnerName();
        this.ownerType = database.getOwnerType();
        this.parameters = database.getParameters();
    }

    @JsonProperty
    public Optional<String> getWriterVersion()
    {
        return writerVersion;
    }

    @JsonProperty
    public Optional<String> getOwnerName()
    {
        return ownerName;
    }

    @JsonProperty
    public Optional<PrincipalType> getOwnerType()
    {
        return ownerType;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    public Database toDatabase(String databaseName, String location)
    {
        return Database.builder()
                .setDatabaseName(databaseName)
                .setLocation(Optional.of(location))
                .setOwnerName(ownerName)
                .setOwnerType(ownerType)
                .setParameters(parameters)
                .build();
    }
}
