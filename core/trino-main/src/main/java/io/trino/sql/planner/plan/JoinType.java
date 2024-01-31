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
package io.trino.sql.planner.plan;

import io.trino.sql.tree.Join;

public enum JoinType
{
    INNER("InnerJoin"),
    LEFT("LeftJoin"),
    RIGHT("RightJoin"),
    FULL("FullJoin");

    private final String joinLabel;

    JoinType(String joinLabel)
    {
        this.joinLabel = joinLabel;
    }

    public String getJoinLabel()
    {
        return joinLabel;
    }

    public static JoinType typeConvert(Join.Type joinType)
    {
        return switch (joinType) {
            case CROSS, IMPLICIT, INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT;
            case RIGHT -> JoinType.RIGHT;
            case FULL -> JoinType.FULL;
        };
    }
}
