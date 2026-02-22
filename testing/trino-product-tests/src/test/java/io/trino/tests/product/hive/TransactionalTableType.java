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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;

import java.util.List;

public enum TransactionalTableType
{
    ACID {
        @Override
        List<String> getHiveTableProperties()
        {
            return ImmutableList.of("'transactional'='true'");
        }

        @Override
        List<String> getTrinoTableProperties()
        {
            return ImmutableList.of("transactional = true");
        }
    },
    INSERT_ONLY {
        @Override
        List<String> getHiveTableProperties()
        {
            return ImmutableList.of("'transactional'='true'", "'transactional_properties'='insert_only'");
        }

        @Override
        List<String> getTrinoTableProperties()
        {
            throw new RuntimeException("insert_only tables not supported");
        }
    },
    /**/;

    abstract List<String> getHiveTableProperties();

    abstract List<String> getTrinoTableProperties();
}
