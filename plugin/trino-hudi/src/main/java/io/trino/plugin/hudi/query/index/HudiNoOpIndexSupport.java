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
package io.trino.plugin.hudi.query.index;

import io.airlift.log.Logger;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.util.Lazy;

/**
 * Noop index support to ensure that MDT enabled split generation is entered.
 */
public class HudiNoOpIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiNoOpIndexSupport.class);

    public HudiNoOpIndexSupport(SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient)
    {
        super(log, schemaTableName, lazyMetaClient);
    }

    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        return true;
    }
}
