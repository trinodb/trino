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
package io.trino.spi.connector;

import io.trino.spi.predicate.Domain;
import io.trino.spi.tesseract.TesseractTableInfo;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a handle to a relation returned from the connector to the engine.
 * It will be used by the engine whenever given relation will be accessed.
 */
public interface ConnectorTableHandle
{

    /**
     *  To fetch table information like name, schema and properties etc . This is not a time consuming Op
     */
    default Optional<TesseractTableInfo> getTesseractTableInfo(){
        return Optional.empty();
    }

    /**
     *  Evaluates the optimal predicate for the partition column's required values , connects with tesseract metadata cache to do so
     */
    default Optional<String> getTesseractTableOptimalPredicate(Optional<Map<ColumnHandle, Domain>> partitionColDomains){
        return Optional.empty();
    }

    default boolean refersTesseractPartitionColumn(Set<ColumnHandle> columns){
        return false;
    }

}
