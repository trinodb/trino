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
package io.trino.plugin.varada.storage.read;

import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.gen.constants.MatchCollectOp;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.util.Objects;

class MatchCollectElement
{
    private final QueryMatchData queryMatchData;
    private final MatchCollectOp op;
    private final int matchCollectIndex;

    public MatchCollectElement(QueryMatchData queryMatchData, WarmUpType collectWarmUpType, int matchCollectIndex, boolean mappedMatchCollect)
    {
        this.queryMatchData = queryMatchData;
        this.op = (collectWarmUpType == WarmUpType.WARM_UP_TYPE_DATA) ? MatchCollectOp.MATCH_COLLECT_OP_DATA : (mappedMatchCollect ? MatchCollectOp.MATCH_COLLECT_OP_MAPPING : MatchCollectOp.MATCH_COLLECT_OP_INDEX);
        this.matchCollectIndex = matchCollectIndex;
    }

    public QueryMatchData getQueryMatchData()
    {
        return queryMatchData;
    }

    public MatchCollectOp getMatchCollectOp()
    {
        return op;
    }

    public int getMatchCollectIndex()
    {
        return matchCollectIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queryMatchData, op, matchCollectIndex);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof MatchCollectElement o)) {
            return false;
        }
        return queryMatchData.equals(o.queryMatchData) && (op == o.op) && (matchCollectIndex == o.matchCollectIndex);
    }

    @Override
    public String toString()
    {
        return "MatchCollectElement{" +
                "queryMatchData=" + queryMatchData +
                ", op=" + op +
                ", matchCollectIndex=" + matchCollectIndex +
                '}';
    }
}
