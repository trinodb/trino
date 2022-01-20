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
package io.trino.operator.hash;

import io.trino.operator.Work;
import io.trino.spi.Page;

import java.lang.reflect.Constructor;

public class WorkFactory<T>
{
    private Constructor<? extends Work<T>> constructor;

    public WorkFactory(Class<? extends Work<T>> workClass)
    {
        try {
            constructor = workClass.getConstructor(HashTableDataGroupByHash.class, GroupByHashTable.class, Page.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public Work<T> create(HashTableDataGroupByHash groupByHash, GroupByHashTable groupByHashTable, Page page)
    {
        try {
            return constructor.newInstance(groupByHash, groupByHashTable, page);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
