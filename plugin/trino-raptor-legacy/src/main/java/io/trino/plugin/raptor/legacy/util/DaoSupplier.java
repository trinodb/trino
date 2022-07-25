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
package io.trino.plugin.raptor.legacy.util;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import static io.trino.plugin.raptor.legacy.util.DatabaseUtil.onDemandDao;
import static java.util.Objects.requireNonNull;

public class DaoSupplier<T>
{
    private final Class<? extends T> type;
    private final T dao;

    public <X extends T> DaoSupplier(Jdbi dbi, Class<X> type)
    {
        requireNonNull(dbi, "dbi is null");
        requireNonNull(type, "type is null");
        this.type = type;
        this.dao = onDemandDao(dbi, type);
    }

    public T onDemand()
    {
        return dao;
    }

    public T attach(Handle handle)
    {
        return handle.attach(type);
    }
}
