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
package io.trino.sql.parser.hive;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Relation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * @author tangyun@bigo.sg
 * @date 12/4/19 3:16 PM
 */
public class LeftSemiJoin
        extends Relation
{
    private final Relation left;
    private final InPredicate inPredicate;

    protected LeftSemiJoin(Optional<NodeLocation> location, Relation left, InPredicate inPredicate)
    {
        super(location);
        this.left = left;
        this.inPredicate = inPredicate;
    }

    public Relation getLeft()
    {
        return left;
    }

    public InPredicate getInPredicate()
    {
        return inPredicate;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(left, inPredicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(left, inPredicate);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LeftSemiJoin leftSemiJoin = (LeftSemiJoin) obj;
        return Objects.equals(leftSemiJoin.left, this.left) &&
                Objects.equals(leftSemiJoin.inPredicate, this.inPredicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("left", left)
                .add("inPredicate", inPredicate)
                .omitNullValues()
                .toString();
    }
}
