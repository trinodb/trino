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
package io.trino.plugin.oracle;

import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;

import java.util.Map;

public class QueryEnhancedInfo
{
    public boolean isNamedRelation;
    public JdbcNamedRelationHandle namedRelation;
    public boolean isJoin;
    public QueryEnhancedInfo leftInfo;
    public QueryEnhancedInfo rightInfo;
    Map<JdbcColumnHandle, String> leftProjections;
    Map<JdbcColumnHandle, String> rightProjections;

    public QueryEnhancedInfo(JdbcNamedRelationHandle namedRelation1)
    {
        namedRelation = namedRelation1;
        isNamedRelation = true;
    }

    public QueryEnhancedInfo(QueryEnhancedInfo leftInfo, QueryEnhancedInfo rightInfo)
    {
        this.isJoin = true;
        this.leftInfo = leftInfo;
        this.rightInfo = rightInfo;
    }

    public QueryEnhancedInfo(QueryEnhancedInfo leftInfo, QueryEnhancedInfo rightInfo,
                             Map<JdbcColumnHandle, String> leftProjections, Map<JdbcColumnHandle, String> rightProjections)
    {
        this.isJoin = true;
        this.leftInfo = leftInfo;
        this.rightInfo = rightInfo;
        this.leftProjections = leftProjections;
        this.rightProjections = rightProjections;
    }
}
