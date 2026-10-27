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
package io.trino.decoder.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import java.util.Optional;

public interface DynamicMessageProvider
{
    /**
     * Parses a record with the schema it was written with.
     */
    DynamicMessage parseDynamicMessage(byte[] data);

    /**
     * The message type the table columns were derived from. Fields of a parsed message of this
     * type are looked up by the number this type assigns to the column's field, so a field keeps
     * its column across renames, as it keeps its identity on the wire. Without a reader type, and
     * in messages of another type, fields are looked up by name.
     */
    default Optional<Descriptor> getReaderDescriptor()
    {
        return Optional.empty();
    }

    interface Factory
    {
        DynamicMessageProvider create(Optional<String> protoFile);
    }
}
