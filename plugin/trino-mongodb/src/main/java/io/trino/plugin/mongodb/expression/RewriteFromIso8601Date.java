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
package io.trino.plugin.mongodb.expression;

import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.DateType;

public class RewriteFromIso8601Date
        extends RewriteFromIso8601Conversion
{
    public RewriteFromIso8601Date()
    {
        super(new FunctionName("from_iso8601_date"), DateType.class::isInstance);
    }
}
