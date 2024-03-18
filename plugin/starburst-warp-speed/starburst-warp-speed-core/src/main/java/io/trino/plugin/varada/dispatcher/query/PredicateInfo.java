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
package io.trino.plugin.varada.dispatcher.query;

import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;

import java.util.List;
import java.util.Objects;

public record PredicateInfo(
        PredicateType predicateType,
        FunctionType functionType,
        int numValues,
        List<Object> functionParams,
        int recTypeLength)
{
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PredicateInfo that)) {
            return false;
        }
        return predicateType == that.predicateType &&
                functionType == that.functionType &&
                functionParams.equals(that.functionParams) &&
                numValues == that.numValues;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(predicateType, functionType, numValues, functionParams);
    }
}
