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
package io.prestosql.server;

import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.security.AccessControl;
import io.prestosql.server.security.ResourceSecurity;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.GroupProvider;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.security.AccessControlUtil.checkCanViewQueryOwnedBy;
import static io.prestosql.security.AccessControlUtil.filterQueries;
import static io.prestosql.server.HttpRequestSessionContext.extractAuthorizedIdentity;
import static io.prestosql.server.QueryStateInfo.createQueryStateInfo;
import static io.prestosql.server.QueryStateInfo.createQueuedQueryStateInfo;
import static io.prestosql.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/queryState")
public class QueryStateInfoResource
{
    private final DispatchManager dispatchManager;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;

    @Inject
    public QueryStateInfoResource(
            DispatchManager dispatchManager,
            ResourceGroupManager<?> resourceGroupManager,
            AccessControl accessControl,
            GroupProvider groupProvider)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<QueryStateInfo> getQueryStateInfos(@QueryParam("user") String user, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        List<BasicQueryInfo> queryInfos = dispatchManager.getQueries();
        queryInfos = filterQueries(extractAuthorizedIdentity(servletRequest, httpHeaders, accessControl, groupProvider), queryInfos, accessControl);

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
            checkCanViewQueryOwnedBy(extractAuthorizedIdentity(servletRequest, httpHeaders, accessControl, groupProvider), queryInfo.getSession().getUser(), accessControl);
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
