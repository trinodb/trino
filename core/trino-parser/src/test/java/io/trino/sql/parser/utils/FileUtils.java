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
package io.trino.sql.parser.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class FileUtils
{
    private FileUtils()
    {
    }

    public static void saveBytesAsFile(byte[] bytes, String path)
    {
        File file = new File(path);

        if (file.exists()) {
            deleteDir(file);
        }
        file = new File(path);
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(file);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            outputStream.write(bytes);
        }
        catch (IOException e) {
        }
    }

    public static List<String> scanDirectory(String rootPath)
    {
        File file = new File(rootPath);
        File[] files = file.listFiles();
        List<String> filePaths = new ArrayList<>();
        for (File file1 : files) {
            filePaths.add(rootPath + "/" + file1.getName());
        }
        return filePaths;
    }

    public static byte[] getFileAsBytes(String path)
    {
        File file = new File(path);
        InputStream inputStream;
        ByteBuffer byteBuffer = new ByteBuffer(1024 * 1024 * 10);
        try {
            inputStream = new FileInputStream(file);

            byte[] tmp = new byte[4096];
            int length;
            while ((length = inputStream.read(tmp, 0, 4096)) > 0) {
                byteBuffer.append(tmp, 0, length);
            }
        }
        catch (FileNotFoundException e) {
        }
        catch (IOException e) {
        }

        return byteBuffer.getData();
    }

    public static byte[] getFileAsBytes(InputStream inputStream)
    {
        ByteBuffer byteBuffer = new ByteBuffer(1024 * 1024 * 10);
        try {
            byte[] tmp = new byte[4096];
            int length;
            while ((length = inputStream.read(tmp, 0, 4096)) > 0) {
                byteBuffer.append(tmp, 0, length);
            }
        }
        catch (FileNotFoundException e) {
        }
        catch (IOException e) {
        }

        return byteBuffer.getData();
    }

    private static boolean deleteDir(File dir)
    {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

    public static void makeSureDirExist(String dir)
    {
        File file = new File(dir);
        if (file.exists()) {
            return;
        }
        file.mkdirs();
    }

    public static void saveFile(String path, InputStream inputStream)
    {
        File file = new File(path);
        if (file.exists()) {
            deleteDir(file);
        }
        file = new File(path);
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(file);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            byte[] bytes = new byte[4096];
            int length = 0;
            while ((length = inputStream.read(bytes)) > 0) {
                outputStream.write(bytes, 0, length);
            }
        }
        catch (IOException e) {
        }
    }

    public static void sendFile(String path, OutputStream outputStream)
    {
        File file = new File(path);
        if (!file.exists()) {
            return;
        }
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            byte[] bytes = new byte[4096];
            int length = 0;
            while ((length = inputStream.read(bytes)) > 0) {
                outputStream.write(bytes, 0, length);
            }
        }
        catch (FileNotFoundException e) {
        }
        catch (IOException e) {
        }
    }
}
