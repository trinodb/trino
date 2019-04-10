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
package io.prestosql.server.extension.query.history;

import io.prestosql.execution.QueryInfo;

import java.io.IOException;
import java.sql.Timestamp;

public class QueryHistory
{
    private final String cluster;
    private final String queryId;
    private final String queryState;
    private final String user;
    private final String source;
    private final String catalog;
    private final Timestamp createTime;
    private final Timestamp endTime;
    private final String query;
    private final String queryInfo;

    public QueryHistory(QueryInfo queryInfo, String cluster) throws IOException
    {
        this.cluster = cluster;
        queryId = queryInfo.getQueryId().getId();
        queryState = queryInfo.getState().name();
        user = queryInfo.getSession().getUser();
        source = queryInfo.getSession().getSource().orElse(null);
        catalog = queryInfo.getSession().getCatalog().orElse(null);
        createTime = new Timestamp(queryInfo.getQueryStats().getCreateTime().getMillis());
        endTime = queryInfo.getQueryStats().getEndTime() == null ?
                null : new Timestamp((queryInfo.getQueryStats().getEndTime().getMillis()));
        query = queryInfo.getQuery();
        this.queryInfo = QueryHistorySQLStore.serializeQueryInfo(queryInfo);
    }

    public String getCluster()
    {
        return cluster;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getQueryState()
    {
        return queryState;
    }

    public String getUser()
    {
        return user;
    }

    public String getSource()
    {
        return source;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public Timestamp getCreateTime()
    {
        return createTime;
    }

    public Timestamp getEndTime()
    {
        return endTime;
    }

    public String getQuery()
    {
        return query;
    }

    public String getQueryInfo()
    {
        return queryInfo;
    }
}
