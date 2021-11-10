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
package io.trino.server.security;

import com.google.common.collect.ImmutableSet;
import io.trino.security.AccessControl;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;

@javax.ws.rs.Path("/username")
public class TestSystemAccessResource
{
    private final HttpRequestSessionContextFactory sessionContextFactory;

    @Inject
    public TestSystemAccessResource(AccessControl accessControl)
    {
        this.sessionContextFactory = new HttpRequestSessionContextFactory(createTestMetadataManager(), ImmutableSet::of, accessControl);
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    public javax.ws.rs.core.Response echoToken(@Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        try {
            Identity identity = sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, Optional.empty());
            return javax.ws.rs.core.Response.ok()
                    .header("user", identity.getUser())
                    .build();
        }
        catch (AccessDeniedException e) {
            throw new ForbiddenException(e.getMessage(), e);
        }
    }
}
