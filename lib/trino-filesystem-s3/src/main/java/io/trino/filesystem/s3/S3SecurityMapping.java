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
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class S3SecurityMapping
{
    private final Predicate<String> user;
    private final Predicate<Collection<String>> group;
    private final Predicate<S3Location> prefix;
    private final Optional<String> iamRole;
    private final Optional<String> roleSessionName;
    private final Set<String> allowedIamRoles;
    private final Optional<String> kmsKeyId;
    private final Set<String> allowedKmsKeyIds;
    private final Optional<String> sseCustomerKey;
    private final Set<String> allowedSseCustomerKeys;
    private final Optional<AwsCredentials> credentials;
    private final boolean useClusterDefault;
    private final Optional<String> endpoint;
    private final Optional<String> region;

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
        this.user = user
                .map(S3SecurityMapping::toPredicate)
                .orElse(_ -> true);
        this.group = group
                .map(S3SecurityMapping::toPredicate)
                .map(S3SecurityMapping::anyMatch)
                .orElse(_ -> true);
        this.prefix = prefix
                .map(Location::of)
                .map(S3Location::new)
                .map(S3SecurityMapping::prefixPredicate)
                .orElse(_ -> true);

        this.iamRole = requireNonNull(iamRole, "iamRole is null");
        this.roleSessionName = requireNonNull(roleSessionName, "roleSessionName is null");
        checkArgument(roleSessionName.isEmpty() || iamRole.isPresent(), "iamRole must be provided when roleSessionName is provided");

        this.allowedIamRoles = ImmutableSet.copyOf(allowedIamRoles.orElse(ImmutableList.of()));

        this.kmsKeyId = requireNonNull(kmsKeyId, "kmsKeyId is null");

        this.allowedKmsKeyIds = ImmutableSet.copyOf(allowedKmsKeyIds.orElse(ImmutableList.of()));

        this.sseCustomerKey = requireNonNull(sseCustomerKey, "sseCustomerKey is null");

        this.allowedSseCustomerKeys = allowedSseCustomerKeys.map(ImmutableSet::copyOf).orElse(ImmutableSet.of());

        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");
        checkArgument(accessKey.isPresent() == secretKey.isPresent(), "accessKey and secretKey must be provided together");
        this.credentials = accessKey.map(access -> AwsBasicCredentials.create(access, secretKey.get()));

        this.useClusterDefault = useClusterDefault.orElse(false);
        boolean roleOrCredentialsArePresent = !this.allowedIamRoles.isEmpty() || iamRole.isPresent() || credentials.isPresent();
        checkArgument(this.useClusterDefault != roleOrCredentialsArePresent, "must either allow useClusterDefault role or provide role and/or credentials");

        checkArgument(!this.useClusterDefault || this.kmsKeyId.isEmpty(), "KMS key ID cannot be provided together with useClusterDefault");
        checkArgument(!this.useClusterDefault || this.sseCustomerKey.isEmpty(), "SSE Customer key cannot be provided together with useClusterDefault");
        checkArgument(this.kmsKeyId.isEmpty() || this.sseCustomerKey.isEmpty(), "SSE Customer key cannot be provided together with KMS key ID");

        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.region = requireNonNull(region, "region is null");
    }

    boolean matches(ConnectorIdentity identity, S3Location location)
    {
        return user.test(identity.getUser()) &&
                group.test(identity.getGroups()) &&
                prefix.test(location);
    }

    public Optional<String> iamRole()
    {
        return iamRole;
    }

    public Optional<String> roleSessionName()
    {
        return roleSessionName;
    }

    public Set<String> allowedIamRoles()
    {
        return allowedIamRoles;
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

    public Optional<AwsCredentials> credentials()
    {
        return credentials;
    }

    public boolean useClusterDefault()
    {
        return useClusterDefault;
    }

    public Optional<String> endpoint()
    {
        return endpoint;
    }

    public Optional<String> region()
    {
        return region;
    }

    private static Predicate<S3Location> prefixPredicate(S3Location prefix)
    {
        return value -> prefix.bucket().equals(value.bucket()) &&
                value.key().startsWith(prefix.key());
    }

    private static Predicate<String> toPredicate(Pattern pattern)
    {
        return value -> pattern.matcher(value).matches();
    }

    private static <T> Predicate<Collection<T>> anyMatch(Predicate<T> predicate)
    {
        return values -> values.stream().anyMatch(predicate);
    }
}
