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
package io.trino.server.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.trino.execution.QueryState;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class TrimmedBasicQueryInfo
{
    private static final int MAX_QUERY_PREVIEW_LENGTH = 300;

    private final QueryId queryId;
    private final String sessionUser;
    private final Optional<String> sessionPrincipal;
    private final Optional<String> sessionSource;
    private final Optional<ResourceGroupId> resourceGroupId;
    private final QueryState state;
    private final boolean scheduled;
    private final URI self;
    private final String queryTextPreview;
    private final Optional<String> updateType;
    private final Optional<String> preparedQuery;
    private final BasicQueryStats queryStats;
    private final Optional<ErrorType> errorType;
    private final Optional<ErrorCode> errorCode;
    private final Optional<QueryType> queryType;
    private final RetryPolicy retryPolicy;

    public TrimmedBasicQueryInfo(BasicQueryInfo queryInfo)
    {
        this.queryId = requireNonNull(queryInfo.getQueryId(), "queryId is null");
        this.sessionUser = requireNonNull(queryInfo.getSession().getUser(), "user is null");
        this.sessionPrincipal = requireNonNull(queryInfo.getSession().getPrincipal(), "principal is null");
        this.sessionSource = requireNonNull(queryInfo.getSession().getSource(), "source is null");
        this.resourceGroupId = requireNonNull(queryInfo.getResourceGroupId(), "resourceGroupId is null");
        this.state = requireNonNull(queryInfo.getState(), "state is null");
        this.errorType = Optional.ofNullable(queryInfo.getErrorType());
        this.errorCode = Optional.ofNullable(queryInfo.getErrorCode());
        this.scheduled = queryInfo.isScheduled();
        this.self = requireNonNull(queryInfo.getSelf(), "self is null");
        String queryText = requireNonNull(queryInfo.getQuery(), "query is null");
        if (queryText.length() <= MAX_QUERY_PREVIEW_LENGTH) {
            this.queryTextPreview = queryText;
        }
        else {
            this.queryTextPreview = queryText.substring(0, MAX_QUERY_PREVIEW_LENGTH - 1) + " ...";
        }
        this.updateType = requireNonNull(queryInfo.getUpdateType(), "updateType is null");
        this.preparedQuery = requireNonNull(queryInfo.getPreparedQuery(), "preparedQuery is null");
        this.queryStats = requireNonNull(queryInfo.getQueryStats(), "queryStats is null");
        this.queryType = requireNonNull(queryInfo.getQueryType(), "queryType is null");
        this.retryPolicy = requireNonNull(queryInfo.getRetryPolicy(), "retryPolicy is null");
    }

    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getSessionUser()
    {
        return sessionUser;
    }

    @JsonProperty
    public Optional<String> getSessionPrincipal()
    {
        return sessionPrincipal;
    }

    @JsonProperty
    public Optional<String> getSessionSource()
    {
        return sessionSource;
    }

    @JsonProperty
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    public QueryState getState()
    {
        return state;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public String getQueryTextPreview()
    {
        return queryTextPreview;
    }

    @JsonProperty
    public Optional<String> getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    public Optional<String> getPreparedQuery()
    {
        return preparedQuery;
    }

    @JsonProperty
    public BasicQueryStats getQueryStats()
    {
        return queryStats;
    }

    @JsonProperty
    public Optional<ErrorType> getErrorType()
    {
        return errorType;
    }

    @JsonProperty
    public Optional<ErrorCode> getErrorCode()
    {
        return errorCode;
    }

    @JsonProperty
    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    @JsonProperty
    public RetryPolicy getRetryPolicy()
    {
        return retryPolicy;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .toString();
    }
}
