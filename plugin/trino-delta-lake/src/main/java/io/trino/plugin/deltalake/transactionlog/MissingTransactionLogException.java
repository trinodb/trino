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

import io.trino.spi.TrinoException;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_TABLE;
import static java.lang.String.format;

public class MissingTransactionLogException
        extends TrinoException
{
    public MissingTransactionLogException(String transactionLogPath)
    {
        super(DELTA_LAKE_INVALID_TABLE, format("The transaction log file %s was not found", transactionLogPath));
    }
}
