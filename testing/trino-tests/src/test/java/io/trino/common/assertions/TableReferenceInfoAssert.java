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
import io.trino.spi.eventlistener.ColumnMaskReferenceInfo;
import io.trino.spi.eventlistener.FilterMaskReferenceInfo;
import io.trino.spi.eventlistener.MaterializedViewReferenceInfo;
import io.trino.spi.eventlistener.RowFilterReferenceInfo;
import io.trino.spi.eventlistener.TableReferenceInfo;
import io.trino.spi.eventlistener.ViewReferenceInfo;
import org.assertj.core.api.AbstractAssert;

import static io.trino.common.assertions.TrinoAssertions.assertThat;

public class TableReferenceInfoAssert
        extends AbstractAssert<TableReferenceInfoAssert, TableReferenceInfo>
{
    TableReferenceInfoAssert(TableReferenceInfo actual)
    {
        super(actual, TableReferenceInfoAssert.class);
    }

    public BaseViewReferenceInfoAssert asViewInfo()
    {
        assertThat(actual).isInstanceOf(ViewReferenceInfo.class);
        return new BaseViewReferenceInfoAssert((BaseViewReferenceInfo) actual);
    }

    public BaseViewReferenceInfoAssert asMaterializedViewInfo()
    {
        assertThat(actual).isInstanceOf(MaterializedViewReferenceInfo.class);
        return new BaseViewReferenceInfoAssert((BaseViewReferenceInfo) actual);
    }

    public FilterMaskReferenceInfoAssert asRowFilterInfo()
    {
        assertThat(actual).isInstanceOf(RowFilterReferenceInfo.class);
        return new FilterMaskReferenceInfoAssert((FilterMaskReferenceInfo) actual);
    }

    public FilterMaskReferenceInfoAssert asColumnMaskInfo()
    {
        assertThat(actual).isInstanceOf(ColumnMaskReferenceInfo.class);
        return new FilterMaskReferenceInfoAssert((FilterMaskReferenceInfo) actual);
    }
}
