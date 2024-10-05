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
package io.trino.plugin.pulsar;

import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;

/**
 * Defines additional API that is used in {@link PulsarRecordCursor} but not present in {@link ReadOnlyCursor}
 * to make it possible to create mock class for testing.
 */
public interface PulsarReadOnlyCursor
        extends ReadOnlyCursor
{
    MLDataFormats.ManagedLedgerInfo.LedgerInfo getCurrentLedgerInfo();
}
