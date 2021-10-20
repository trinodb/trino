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
package io.trino.sql.parser;

import static java.util.Objects.requireNonNull;

public class ParsingOptions
{
    public enum DecimalLiteralTreatment
    {
        AS_DOUBLE,
        AS_DECIMAL,
        REJECT
    }

    public enum SqlParserMode
    {
        READ_ONLY,
        CRUD
    }

    private final DecimalLiteralTreatment decimalLiteralTreatment;
    private final SqlParserMode parserMode;

    public ParsingOptions()
    {
        this(DecimalLiteralTreatment.REJECT);
    }

    public ParsingOptions(DecimalLiteralTreatment decimalLiteralTreatment)
    {
        this.parserMode = requireNonNull(SqlParserMode.CRUD, "parserMode is null");
        this.decimalLiteralTreatment = requireNonNull(decimalLiteralTreatment, "decimalLiteralTreatment is null");
    }

    public ParsingOptions(DecimalLiteralTreatment decimalLiteralTreatment, SqlParserMode parserMode)
    {
        this.parserMode = requireNonNull(parserMode, "parserMode is null");
        this.decimalLiteralTreatment = requireNonNull(decimalLiteralTreatment, "decimalLiteralTreatment is null");
    }

    public DecimalLiteralTreatment getDecimalLiteralTreatment()
    {
        return decimalLiteralTreatment;
    }

    public SqlParserMode getSqlParserMode()
    {
        return parserMode;
    }
}
