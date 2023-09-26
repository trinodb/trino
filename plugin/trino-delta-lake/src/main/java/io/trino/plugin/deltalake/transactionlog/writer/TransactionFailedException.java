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
package io.trino.plugin.deltalake.transactionlog.writer;

/**
 * Exception thrown to point out that other transaction log files
 * have been created during the time on which the current transaction
 * has been executing and their already committed changes are in
 * irremediable conflict with the changes from the current transaction.
 */
public class TransactionFailedException
        extends RuntimeException
{
    public TransactionFailedException(String message)
    {
        super(message);
    }
}
