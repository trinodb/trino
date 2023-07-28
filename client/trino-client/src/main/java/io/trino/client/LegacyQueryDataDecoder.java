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
package io.trino.client;

import jakarta.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.client.FixJsonDataUtils.fixData;

public class LegacyQueryDataDecoder
        implements QueryDataDecoder
{
    @Override
    public Iterable<List<Object>> decode(@Nullable QueryData queryData, List<Column> columns)
    {
        if (queryData == null) {
            return null;
        }

        verify(queryData instanceof LegacyQueryData, "LegacyQueryData was expected but got %s", queryData.getClass());
        return fixData(columns, queryData.getData());
    }
}
