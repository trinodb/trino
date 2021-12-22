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
package io.trino.sql.parser.hive;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.parser.utils.FileUtils;
import io.trino.sql.tree.Node;

import java.nio.charset.Charset;

import static org.testng.Assert.assertEquals;

public abstract class SQLTester
{
    private static SqlParser sqlParser = new SqlParser();
    private static ParsingOptions hiveParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private static ParsingOptions prestoParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    protected Node useHiveParser(String sql)
    {
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        return node;
    }

    protected Node usePrestoParser(String sql)
    {
        return sqlParser.createStatement(sql, prestoParsingOptions);
    }

    protected void checkASTNode(String prestoSql, String hiveSql)
    {
        Node prestoNode = usePrestoParser(prestoSql);
        System.out.println(prestoNode);

        Node hiveNode = useHiveParser(hiveSql);
        System.out.println(hiveNode);

        System.out.println(hiveNode.toString().replaceAll("\"", ""));
        System.out.println(prestoNode.toString());
        assertEquals(hiveNode, prestoNode);
    }

    protected void checkTypeASTNode(String prestoSql, String hiveSql)
    {
        Node prestoNode = sqlParser.createType(prestoSql, prestoParsingOptions);
        System.out.println(prestoNode);

        Node hiveNode = sqlParser.createType(hiveSql, hiveParsingOptions);
        System.out.println(hiveNode);

        assertEquals(hiveNode, prestoNode);
    }

    protected void checkASTNode(Node prestoNode, Node hiveNode)
    {
        System.out.println(prestoNode);
        System.out.println(hiveNode);

        assertEquals(hiveNode, prestoNode);
    }

    protected void checkASTNode(String sql)
    {
        checkASTNode(sql, sql);
    }

    protected Node runHiveSQL(String hiveSql)
    {
        return useHiveParser(hiveSql);
    }

    protected Node runPrestoSQL(String prestoSql)
    {
        return usePrestoParser(prestoSql);
    }

    protected void checkASTNodeFromFile(String prestoPath, String hivePath)
    {
        String prestoSql = getResourceContent(prestoPath);
        String hiveSql = getResourceContent(hivePath);
        checkASTNode(prestoSql, hiveSql);
    }

    protected void checkASTNodeFromFile(String sqlPath)
    {
        String sql = getResourceContent(sqlPath);
        checkASTNode(sql, sql);
    }

    protected Node runHiveSQLFromFile(String hiveSqlPath)
    {
        String hiveSql = getResourceContent(hiveSqlPath);
        return useHiveParser(hiveSql);
    }

    protected Node runPrestoSQLFromFile(String prestoSqlPath)
    {
        String prestoSql = getResourceContent(prestoSqlPath);
        return usePrestoParser(prestoSql);
    }

    static {
        hiveParsingOptions.setIfUseHiveParser(true);
        prestoParsingOptions.setIfUseHiveParser(false);
    }

    protected String getResourceContent(String path)
    {
        String fullPath =
                this.getClass()
                        .getResource("../../../../../")
                        .getFile() + path;
        return new String(FileUtils.getFileAsBytes(fullPath), Charset.defaultCharset());
    }

    protected void traversalAstTree(Node node, NodeAction nodeAction)
    {
        nodeAction.action(node);
        for (Node child : node.getChildren()) {
            traversalAstTree(child, nodeAction);
        }
    }

    public interface NodeAction
    {
        void action(Node node);
    }
}
