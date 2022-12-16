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
package io.trino.plugin.hive.metastore.thrift;

import java.util.concurrent.atomic.AtomicInteger;

public class ThriftHiveMetastoreIntrospection
{
    public static final int UNDEFINED_ALTERNATIVE = Integer.MAX_VALUE;

    private final MetastoreSupportsDateStatistics metastoreSupportsDateStatistics = new MetastoreSupportsDateStatistics();
    private final AtomicInteger chosenGetTableAlternative = new AtomicInteger(UNDEFINED_ALTERNATIVE);
    private final AtomicInteger chosenTableParamAlternative = new AtomicInteger(UNDEFINED_ALTERNATIVE);
    private final AtomicInteger chosenGetAllViewsAlternative = new AtomicInteger(UNDEFINED_ALTERNATIVE);
    private final AtomicInteger chosenAlterTransactionalTableAlternative = new AtomicInteger(UNDEFINED_ALTERNATIVE);
    private final AtomicInteger chosenAlterPartitionsAlternative = new AtomicInteger(UNDEFINED_ALTERNATIVE);

    public MetastoreSupportsDateStatistics getMetastoreSupportsDateStatistics()
    {
        return metastoreSupportsDateStatistics;
    }

    public AtomicInteger getChosenGetTableAlternative()
    {
        return chosenGetTableAlternative;
    }

    public AtomicInteger getChosenTableParamAlternative()
    {
        return chosenTableParamAlternative;
    }

    public AtomicInteger getChosenGetAllViewsAlternative()
    {
        return chosenGetAllViewsAlternative;
    }

    public AtomicInteger getChosenAlterTransactionalTableAlternative()
    {
        return chosenAlterTransactionalTableAlternative;
    }

    public AtomicInteger getChosenAlterPartitionsAlternative()
    {
        return chosenAlterPartitionsAlternative;
    }
}
