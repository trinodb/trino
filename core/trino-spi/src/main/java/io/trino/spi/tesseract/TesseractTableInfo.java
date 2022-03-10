/**
 * Licensed to the Airtel International LLP (AILLP) under one
 * or more contributor license agreements.
 * The AILLP licenses this file to you under the AA License, Version 1.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Akash Roy
 * @department Big Data Analytics Airtel Africa
 * @since Sat, 05-02-2022
 */
package io.trino.spi.tesseract;


import java.util.Map;

/**
 * Store commonly required info for a tesseract table
 */
public class TesseractTableInfo {


    public static final String TESSERACT_OPTIMIZED_TABLE = "optimize-tesseract-queries";

    private String schema;
    private String tableName;
    private Map<String, String> tableProperties;


    public TesseractTableInfo(String schema,String tableName, Map<String, String> tableProperties) {
        this.schema = schema;
        this.tableName = tableName;
        this.tableProperties = tableProperties;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getTableProperties() {
        return tableProperties;
    }

    public void setTableProperties(Map<String, String> tableProperties) {
        this.tableProperties = tableProperties;
    }

    public boolean isTesseractOptimizedTable(){
        return Boolean.parseBoolean(this.getTableProperties().getOrDefault(TESSERACT_OPTIMIZED_TABLE,"false"));
    }
}
