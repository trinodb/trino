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
package io.prestosql.orc.metadata;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class OrcColumnId
        implements Comparable<OrcColumnId>
{
    public static final OrcColumnId ROOT_COLUMN = new OrcColumnId(0);
    private final int id;

    public OrcColumnId(int id)
    {
        checkArgument(id >= 0, "id is negative");
        this.id = id;
    }

    public int getId()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrcColumnId that = (OrcColumnId) o;
        return id == that.id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public int compareTo(OrcColumnId o)
    {
        return Integer.compare(id, o.id);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .toString();
    }
}
