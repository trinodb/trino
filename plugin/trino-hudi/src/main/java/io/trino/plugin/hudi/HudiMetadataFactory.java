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

package io.trino.plugin.hudi;

import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HudiMetadataFactory
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    @Inject
    public HudiMetadataFactory(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, TypeManager typeManager)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public HudiMetadata create()
    {
        return new HudiMetadata(metastore, hdfsEnvironment, typeManager);
    }
}
