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
package io.trino.operator;

import io.trino.operator.join.LookupJoinOperatorFactory;
import io.trino.sql.planner.plan.JoinNode;

import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.FULL_OUTER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.INNER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.LOOKUP_OUTER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;

public class JoinOperatorType
{
    private final LookupJoinOperatorFactory.JoinType type;
    private final boolean outputSingleMatch;
    private final boolean waitForBuild;

    public static JoinOperatorType ofJoinNodeType(JoinNode.Type joinNodeType, boolean outputSingleMatch, boolean waitForBuild)
    {
        return switch (joinNodeType) {
            case INNER -> innerJoin(outputSingleMatch, waitForBuild);
            case LEFT -> probeOuterJoin(outputSingleMatch);
            case RIGHT -> lookupOuterJoin(waitForBuild);
            case FULL -> fullOuterJoin();
        };
    }

    public static JoinOperatorType innerJoin(boolean outputSingleMatch, boolean waitForBuild)
    {
        return new JoinOperatorType(INNER, outputSingleMatch, waitForBuild);
    }

    public static JoinOperatorType probeOuterJoin(boolean outputSingleMatch)
    {
        return new JoinOperatorType(PROBE_OUTER, outputSingleMatch, false);
    }

    public static JoinOperatorType lookupOuterJoin(boolean waitForBuild)
    {
        return new JoinOperatorType(LOOKUP_OUTER, false, waitForBuild);
    }

    public static JoinOperatorType fullOuterJoin()
    {
        return new JoinOperatorType(FULL_OUTER, false, false);
    }

    private JoinOperatorType(LookupJoinOperatorFactory.JoinType type, boolean outputSingleMatch, boolean waitForBuild)
    {
        this.type = requireNonNull(type, "type is null");
        this.outputSingleMatch = outputSingleMatch;
        this.waitForBuild = waitForBuild;
    }

    public boolean isOutputSingleMatch()
    {
        return outputSingleMatch;
    }

    public boolean isWaitForBuild()
    {
        return waitForBuild;
    }

    public LookupJoinOperatorFactory.JoinType getType()
    {
        return type;
    }
}
