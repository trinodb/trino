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
package org.apache.iceberg.aws.dynamodb;

import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

// This is a shim class to make 'initialize' method public
public class IcebergDynamoDbCatalog
        extends org.apache.iceberg.aws.dynamodb.DynamoDbCatalog
{
    @Override
    public void initialize(String catalogName, String path, AwsProperties awsProperties, DynamoDbClient client, FileIO fileIo)
    {
        super.initialize(catalogName, path, awsProperties, client, fileIo);
    }
}
