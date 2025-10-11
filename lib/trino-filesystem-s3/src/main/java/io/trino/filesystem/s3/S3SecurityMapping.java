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
package io.trino.filesystem.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.iam.aws.IAMSecurityMapping;
import io.trino.spi.security.ConnectorIdentity;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class S3SecurityMapping
        extends IAMSecurityMapping
{
    private final Predicate<S3Location> prefix;
    private final Optional<String> kmsKeyId;
    private final Set<String> allowedKmsKeyIds;
    private final Optional<String> sseCustomerKey;
    private final Set<String> allowedSseCustomerKeys;

    @JsonCreator
    public S3SecurityMapping(
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("group") Optional<Pattern> group,
            @JsonProperty("prefix") Optional<String> prefix,
            @JsonProperty("iamRole") Optional<String> iamRole,
            @JsonProperty("roleSessionName") Optional<String> roleSessionName,
            @JsonProperty("allowedIamRoles") Optional<List<String>> allowedIamRoles,
            @JsonProperty("kmsKeyId") Optional<String> kmsKeyId,
            @JsonProperty("allowedKmsKeyIds") Optional<List<String>> allowedKmsKeyIds,
            @JsonProperty("sseCustomerKey") Optional<String> sseCustomerKey,
            @JsonProperty("allowedSseCustomerKeys") Optional<List<String>> allowedSseCustomerKeys,
            @JsonProperty("accessKey") Optional<String> accessKey,
            @JsonProperty("secretKey") Optional<String> secretKey,
            @JsonProperty("useClusterDefault") Optional<Boolean> useClusterDefault,
            @JsonProperty("endpoint") Optional<String> endpoint,
            @JsonProperty("region") Optional<String> region)
    {
        super(user, group, iamRole, roleSessionName, allowedIamRoles, accessKey, secretKey, useClusterDefault, endpoint, region);

        this.prefix = prefix
                .map(Location::of)
                .map(S3Location::new)
                .map(S3SecurityMapping::prefixPredicate)
                .orElse(_ -> true);

        this.kmsKeyId = requireNonNull(kmsKeyId, "kmsKeyId is null");

        this.allowedKmsKeyIds = ImmutableSet.copyOf(allowedKmsKeyIds.orElse(ImmutableList.of()));

        this.sseCustomerKey = requireNonNull(sseCustomerKey, "sseCustomerKey is null");

        this.allowedSseCustomerKeys = allowedSseCustomerKeys.map(ImmutableSet::copyOf).orElse(ImmutableSet.of());

        checkArgument(!this.useClusterDefault || this.kmsKeyId.isEmpty(), "KMS key ID cannot be provided together with useClusterDefault");
        checkArgument(!this.useClusterDefault || this.sseCustomerKey.isEmpty(), "SSE Customer key cannot be provided together with useClusterDefault");
        checkArgument(this.kmsKeyId.isEmpty() || this.sseCustomerKey.isEmpty(), "SSE Customer key cannot be provided together with KMS key ID");
    }

    boolean matches(ConnectorIdentity identity, S3Location location)
    {
        return super.matches(identity) && prefix.test(location);
    }

    public Optional<String> kmsKeyId()
    {
        return kmsKeyId;
    }

    public Set<String> allowedKmsKeyIds()
    {
        return allowedKmsKeyIds;
    }

    public Optional<String> sseCustomerKey()
    {
        return sseCustomerKey;
    }

    public Set<String> allowedSseCustomerKeys()
    {
        return allowedSseCustomerKeys;
    }

    private static Predicate<S3Location> prefixPredicate(S3Location prefix)
    {
        return value -> prefix.bucket().equals(value.bucket()) &&
                value.key().startsWith(prefix.key());
    }
}
