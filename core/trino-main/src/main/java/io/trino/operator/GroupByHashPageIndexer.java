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

import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.operator.UpdateMemory.NOOP;
import static java.util.Objects.requireNonNull;

public class GroupByHashPageIndexer
        implements PageIndexer
{
    private final GroupByHash hash;

    public GroupByHashPageIndexer(List<Type> hashTypes, JoinCompiler joinCompiler, TypeOperators typeOperators)
    {
        this(GroupByHash.createGroupByHash(false, hashTypes, false, 20, false, joinCompiler, typeOperators, NOOP));
    }

    public GroupByHashPageIndexer(GroupByHash hash)
    {
        this.hash = requireNonNull(hash, "hash is null");
    }

    @Override
    public int[] indexPage(Page page)
    {
        Work<int[]> work = hash.getGroupIds(page);
        boolean done = work.process();
        // TODO: this class does not yield wrt memory limit; enable it
        verify(done);
        return work.getResult();
    }

    @Override
    public int getMaxIndex()
    {
        return hash.getGroupCount() - 1;
    }
}
