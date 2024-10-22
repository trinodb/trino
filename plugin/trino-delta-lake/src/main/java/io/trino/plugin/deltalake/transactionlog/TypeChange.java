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
package io.trino.plugin.deltalake.transactionlog;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// https://github.com/delta-io/delta/blob/master/protocol_rfcs/type-widening.md
public record TypeChange(String fromType, String toType, long tableVersion)
{
    public TypeChange
    {
        requireNonNull(fromType, "fromType is null");
        requireNonNull(toType, "toType is null");
        checkArgument(tableVersion >= 0, "tableVersion must be greater than or equal to zero");
    }
}
