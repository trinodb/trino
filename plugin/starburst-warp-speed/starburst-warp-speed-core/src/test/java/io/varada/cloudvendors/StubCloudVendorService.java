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
package io.varada.cloudvendors;

import com.google.inject.Singleton;
import io.varada.cloudvendors.model.StorageObjectMetadata;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

//do not delete - it's being used outside this repo
@Singleton
public class StubCloudVendorService
        extends CloudVendorService
{
    public StubCloudVendorService() {}

    @Override
    public void uploadToCloud(byte[] bytes, String path)
    {}

    @Override
    public void uploadFileToCloud(String localInputPath, String s3OutputPath)
    {}

    @Override
    public boolean uploadFileToCloud(String path, File file, Callable<Boolean> validateBeforeDo)
    {
        return true;
    }

    @Override
    public Optional<String> downloadCompressedFromCloud(
            String path,
            boolean allowKeyNotFound)
    {
        return Optional.empty();
    }

    @Override
    public InputStream downloadRangeFromCloud(String path, long startOffset, int length)
    {
        return InputStream.nullInputStream();
    }

    @Override
    public void downloadFileFromCloud(String path, File file)
    {
    }

    @Override
    public boolean appendOnCloud(String path, File file, long startOffset, boolean isSparseFile, Callable<Boolean> validateBeforeDo)
    {
        return false;
    }

    @Override
    public List<String> listPath(String path, boolean isTopLevel)
    {
        return List.of();
    }

    @Override
    public boolean directoryExists(String cloudPath)
    {
        return true;
    }

//    @Override
//    public boolean doesObjectExist(String path)
//    {
//        return false;
//    }

    @Override
    public StorageObjectMetadata getObjectMetadata(String path)
    {
        return null;
    }

    @Override
    public Optional<Long> getLastModified(String path)
    {
        return Optional.empty();
    }
}
