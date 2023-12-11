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
package io.trino.server.dataframe;

import com.google.inject.Inject;
import com.starburstdata.dataframe.analyzer.TrinoMetadata;
import io.trino.Session;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;

import static java.util.Objects.requireNonNull;

public class TestingTrinoMetadataFactory
{
    private final AnalyzerFactory analyzerFactory;
    private final SqlParser sqlParser;
    private final DataTypeMapper dataTypeMapper;

    @Inject
    public TestingTrinoMetadataFactory(
            AnalyzerFactory analyzerFactory,
            SqlParser sqlParser,
            DataTypeMapper dataTypeMapper)
    {
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.dataTypeMapper = requireNonNull(dataTypeMapper, "dataTypeMapper is null");
    }

    public TrinoMetadata create(Session session)
    {
        return new DataframeMetadataProvider(
                session,
                analyzerFactory,
                sqlParser,
                dataTypeMapper);
    }
}
