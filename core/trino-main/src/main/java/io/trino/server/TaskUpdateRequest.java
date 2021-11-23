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
package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.SessionRepresentation;
import io.trino.execution.TaskSource;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TaskUpdateRequest
{
    private final SessionRepresentation session;
    // extraCredentials is stored separately from SessionRepresentation to avoid being leaked
    private final Map<String, String> extraCredentials;
    // exchangeSecretKey is stored separately from SessionRepresentation to avoid being leaked
    private final Optional<SecretKey> exchangeSecretKey;
    private final Optional<PlanFragment> fragment;
    private final List<TaskSource> sources;
    private final OutputBuffers outputIds;
    private final Map<DynamicFilterId, Domain> dynamicFilterDomains;

    @JsonCreator
    public TaskUpdateRequest(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("extraCredentials") Map<String, String> extraCredentials,
            @JsonProperty("exchangeSecretKey") Optional<SecretKey> exchangeSecretKey,
            @JsonProperty("fragment") Optional<PlanFragment> fragment,
            @JsonProperty("sources") List<TaskSource> sources,
            @JsonProperty("outputIds") OutputBuffers outputIds,
            @JsonProperty("dynamicFilterDomains") Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        requireNonNull(session, "session is null");
        requireNonNull(extraCredentials, "extraCredentials is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputIds, "outputIds is null");
        requireNonNull(dynamicFilterDomains, "dynamicFilterDomains is null");

        this.session = session;
        this.extraCredentials = extraCredentials;
        this.exchangeSecretKey = exchangeSecretKey;
        this.fragment = fragment;
        this.sources = ImmutableList.copyOf(sources);
        this.outputIds = outputIds;
        this.dynamicFilterDomains = dynamicFilterDomains;
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @JsonProperty
    public Optional<PlanFragment> getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public List<TaskSource> getSources()
    {
        return sources;
    }

    @JsonProperty
    public OutputBuffers getOutputIds()
    {
        return outputIds;
    }

    @JsonProperty
    public Map<DynamicFilterId, Domain> getDynamicFilterDomains()
    {
        return dynamicFilterDomains;
    }

    @JsonProperty
    @JsonSerialize(using = SecretKeySerializer.class)
    @JsonDeserialize(using = SecretKeyDeserializer.class)
    public Optional<SecretKey> getExchangeSecretKey()
    {
        return exchangeSecretKey;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("session", session)
                .add("extraCredentials", extraCredentials.keySet())
                .add("fragment", fragment)
                .add("sources", sources)
                .add("outputIds", outputIds)
                .add("dynamicFilterDomains", dynamicFilterDomains)
                .toString();
    }

    public static class SecretKeySerializer
            extends JsonSerializer<Optional<SecretKey>>
    {
        @Override
        public void serialize(Optional<SecretKey> secretKey, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            if (secretKey.isPresent()) {
                jsonGenerator.writeString(Base64.getEncoder().encodeToString(secretKey.get().getEncoded()));
            }
            else {
                jsonGenerator.writeString("");
            }
        }
    }

    public static class SecretKeyDeserializer
            extends JsonDeserializer<Optional<SecretKey>>
    {
        @Override
        public Optional<SecretKey> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            String encodedKey = jsonParser.getValueAsString();
            if (encodedKey.isEmpty()) {
                return Optional.empty();
            }
            else {
                byte[] decodedKey = Base64.getDecoder().decode(encodedKey);
                return Optional.of(new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES"));
            }
        }
    }
}
