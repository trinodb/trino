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
package io.trino.plugin.metastore;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.statestore.StateStore;

import static io.trino.plugin.metastore.MetaStoreConstants.GLOBAL;
import static io.trino.plugin.metastore.MetaStoreConstants.LOCAL;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class BinderStrategy
{
    public void getBinderByType(Binder binder, StateStore stateStore, String type)
    {
        switch (type) {
            case GLOBAL:
                binder.bind(StateStore.class).toInstance(stateStore);
                binder.bind(HetuMetastore.class).to(HetuMetaStoreGlobalCache.class);
                newExporter(binder).export(HetuMetastore.class).as(generator -> generatedNameOf(HetuMetaStoreGlobalCache.class));
                break;
            case LOCAL:
                binder.bind(HetuMetastore.class).to(HetuMetastoreLocalCache.class).in(Scopes.SINGLETON);
                newExporter(binder).export(HetuMetastore.class).as(generator -> generatedNameOf(HetuMetastoreLocalCache.class));
                break;
            default:
                binder.bind(HetuMetastore.class).to(HetuMetastoreNone.class);
                newExporter(binder).export(HetuMetastore.class).as(generator -> generatedNameOf(HetuMetastoreNone.class));
        }
    }
}
