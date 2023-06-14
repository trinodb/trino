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

import com.google.inject.Inject;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.security.AccessControl;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.AccessDeniedException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.security.AccessControlUtil.checkCanViewQueryOwnedBy;
import static io.trino.security.AccessControlUtil.filterQueries;
import static io.trino.server.QueryStateInfo.createQueryStateInfo;
import static io.trino.server.QueryStateInfo.createQueuedQueryStateInfo;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static java.util.Objects.requireNonNull;

@Path("/v1/queryState")
public class QueryStateInfoResource
{
    private final DispatchManager dispatchManager;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final AccessControl accessControl;
    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final Optional<String> alternateHeaderName;

    @Inject
    public QueryStateInfoResource(
            DispatchManager dispatchManager,
            ResourceGroupManager<?> resourceGroupManager,
            AccessControl accessControl,
            HttpRequestSessionContextFactory sessionContextFactory,
            ProtocolConfig protocolConfig)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<QueryStateInfo> getQueryStateInfos(@QueryParam("user") String user, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        List<BasicQueryInfo> queryInfos = dispatchManager.getQueries();
        queryInfos = filterQueries(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, alternateHeaderName), queryInfos, accessControl);

        if (!isNullOrEmpty(user)) {
            queryInfos = queryInfos.stream()
                    .filter(queryInfo -> Pattern.matches(user, queryInfo.getSession().getUser()))
                    .collect(toImmutableList());
        }

        return queryInfos.stream()
                .filter(queryInfo -> !queryInfo.getState().isDone())
                .map(this::getQueryStateInfo)
                .collect(toImmutableList());
    }

    private QueryStateInfo getQueryStateInfo(BasicQueryInfo queryInfo)
    {
        Optional<ResourceGroupId> groupId = queryInfo.getResourceGroupId();
        if (queryInfo.getState() == QUEUED) {
            return createQueuedQueryStateInfo(
                    queryInfo,
                    groupId,
                    groupId.map(group -> resourceGroupManager.tryGetPathToRoot(group)
                            .orElseThrow(() -> new IllegalStateException("Resource group not found: " + group))));
        }
        return createQueryStateInfo(queryInfo, groupId);
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    @Path("{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public QueryStateInfo getQueryStateInfo(@PathParam("queryId") String queryId, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
            throws WebApplicationException
    {
        try {
            BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(new QueryId(queryId));
            checkCanViewQueryOwnedBy(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, alternateHeaderName), queryInfo.getSession().toIdentity(), accessControl);
            return getQueryStateInfo(queryInfo);
        }
        catch (AccessDeniedException e) {
            throw new ForbiddenException();
        }
        catch (NoSuchElementException e) {
            throw new WebApplicationException(NOT_FOUND);
        }
    }
}
