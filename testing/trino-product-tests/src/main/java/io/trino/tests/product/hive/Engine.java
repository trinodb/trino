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

import io.trino.tempto.query.QueryExecutor;

import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public enum Engine
{
    HIVE {
        @Override
        public QueryExecutor queryExecutor()
        {
            return onHive();
        }
    },
    TRINO {
        @Override
        public QueryExecutor queryExecutor()
        {
            return onTrino();
        }
    },
    /**/;

    public abstract QueryExecutor queryExecutor();
}
