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
package io.trino.filesystem.gcs;

import com.google.auth.Credentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.ConnectorIdentity;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

final class IdentityOAuthCredentials
        extends Credentials
{
    private final Supplier<ConnectorIdentity> identitySupplier;

    IdentityOAuthCredentials(Supplier<ConnectorIdentity> identitySupplier)
    {
        this.identitySupplier = requireNonNull(identitySupplier, "identitySupplier is null");
    }

    @Override
    public String getAuthenticationType()
    {
        return "OAuth2";
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri)
    {
        String accessToken = identitySupplier.get().getExtraCredentials().get(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);
        checkState(accessToken != null, "Extra credentials do not contain GCS OAuth token");
        return ImmutableMap.of(AUTHORIZATION, ImmutableList.of("Bearer " + accessToken));
    }

    @Override
    public boolean hasRequestMetadata()
    {
        return true;
    }

    @Override
    public boolean hasRequestMetadataOnly()
    {
        return true;
    }

    @Override
    public void refresh() {}
}
