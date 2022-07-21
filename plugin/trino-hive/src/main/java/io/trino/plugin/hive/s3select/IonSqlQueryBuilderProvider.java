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
package io.trino.plugin.hive.s3select;

import io.trino.plugin.hive.s3select.csv.CsvIonSqlQueryBuilder;
import io.trino.plugin.hive.s3select.json.JsonIonSqlQueryBuilder;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

public class IonSqlQueryBuilderProvider
{
    public Optional<IonSqlQueryBuilder> get(TypeManager typeManager, S3SelectDataType dataType)
    {
        switch (dataType) {
            case CSV:
                return Optional.of(new CsvIonSqlQueryBuilder(typeManager));
            case JSON:
                return Optional.of(new JsonIonSqlQueryBuilder(typeManager));
            default:
                return Optional.empty();
        }
    }
}
