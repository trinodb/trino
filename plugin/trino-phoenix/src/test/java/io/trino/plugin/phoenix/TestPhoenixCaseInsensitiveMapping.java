///*
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.trino.plugin.phoenix;
//
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableMap;
//import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
//import io.trino.testing.QueryRunner;
//import io.trino.testing.sql.SqlExecutor;
//import io.trino.tpch.TpchTable;
//import org.testng.annotations.Test;
//
//import java.nio.file.Path;
//
//import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
//import static java.util.Objects.requireNonNull;
//
//// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
//// Some tests here create colliding names which can cause any other concurrent test to fail.
//@Test(singleThreaded = true)
//public class TestPhoenixCaseInsensitiveMapping
//        extends BaseCaseInsensitiveMappingTest
//{
//    private Path mappingFile;
//    private TestingPhoenixServer phoenixServer;
//
//    @Override
//    protected QueryRunner createQueryRunner()
//            throws Exception
//    {
//        mappingFile = createRuleBasedIdentifierMappingFile();
//        phoenixServer = closeAfterClass(TestingPhoenixServer.getInstance());
//        return PhoenixQueryRunner.createPhoenixQueryRunner(
//                phoenixServer,
//                ImmutableMap.of("http-server.http.port", "8080"),
//                TpchTable.getTables());
//    }
//
//    @Override
//    protected Path getMappingFile()
//    {
//        return requireNonNull(mappingFile, "mappingFile is null");
//    }
//
//    @Override
//    protected SqlExecutor onRemoteDatabase()
//    {
//        requireNonNull(phoenixServer, "phoenixServer is null");
//        return new PhoenixSqlExecutor(phoenixServer.getJdbcUrl());
//    }
//
//    @Test
//    public void forceTestNgToRespectSingleThreaded()
//    {
//        // TODO: Remove after updating TestNG to 7.4.0+ (https://github.com/trinodb/trino/issues/8571)
//        // TestNG doesn't enforce @Test(singleThreaded = true) when tests are defined in base class. According to
//        // https://github.com/cbeust/testng/issues/2361#issuecomment-688393166 a workaround it to add a dummy test to the leaf test class.
//    }
//}
