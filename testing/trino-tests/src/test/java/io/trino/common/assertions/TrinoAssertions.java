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
package io.trino.common.assertions;

import io.trino.spi.eventlistener.BaseViewReferenceInfo;
import io.trino.spi.eventlistener.FilterMaskReferenceInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.eventlistener.TableReferenceInfo;

public class TrinoAssertions
{
    private TrinoAssertions() {}

    public static TableInfoAssert assertThat(TableInfo actual)
    {
        return new TableInfoAssert(actual);
    }

    public static TableReferenceInfoAssert assertThat(TableReferenceInfo actual)
    {
        return new TableReferenceInfoAssert(actual);
    }

    public static BaseViewReferenceInfoAssert assertThat(BaseViewReferenceInfo actual)
    {
        return new BaseViewReferenceInfoAssert(actual);
    }

    public static FilterMaskReferenceInfoAssert assertThat(FilterMaskReferenceInfo actual)
    {
        return new FilterMaskReferenceInfoAssert(actual);
    }
}
