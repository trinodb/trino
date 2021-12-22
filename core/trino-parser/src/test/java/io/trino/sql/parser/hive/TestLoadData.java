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

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

/**
 * @author tangyun@bigo.sg
 * @date 1/6/20 4:07 PM
 */
public class TestLoadData
        extends SQLTester
{
    @Test
    public void test01()
    {
        String sql = "load data inpath 'location' overwrite into table tbl";
        Node sqlNode = runHiveSQL(sql);
        LoadData loadData = new LoadData(
                Optional.empty(),
                QualifiedName.of("tbl"),
                "'location'",
                true,
                ImmutableList.of());
        assertEquals(sqlNode, loadData);
    }

    @Test
    public void test02()
    {
        String sql = "load data inpath 'location' into table tbl";
        Node sqlNode = runHiveSQL(sql);
        LoadData loadData = new LoadData(
                Optional.empty(),
                QualifiedName.of("tbl"),
                "'location'",
                false,
                ImmutableList.of());
        assertEquals(sqlNode, loadData);
    }

    @Test
    public void test03()
    {
        String sql = "load data inpath 'location' into table tbl partition(a='a',b='b',c='c')";
        LoadData sqlNode = (LoadData) runHiveSQL(sql);
        assertEquals("/a=a/b=b/c=c/", sqlNode.getPartitionEnd());
    }
}
