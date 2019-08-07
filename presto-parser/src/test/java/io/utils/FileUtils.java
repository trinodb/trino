package io.utils;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {

    public static void saveBytesAsFile(byte[] bytes, String path) {

        File file = new File(path);

        if (file.exists()) {
            deleteDir(file);
        }
        file = new File(path);
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            outputStream.write(bytes);
        } catch (IOException e) {
        }
    }

    public static List<String> scanDirectory(String rootPath) {

        File file = new File(rootPath);
        File[] files = file.listFiles();
        List<String> filePaths = new ArrayList<>();
        for (File file1: files) {
            filePaths.add(rootPath + "/" + file1.getName());
        }
        return filePaths;
    }

    public static byte[] getFileAsBytes(String path) {

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
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }

        return byteBuffer.getData();
    }

    public static byte[] getFileAsBytes(InputStream inputStream) {
        ByteBuffer byteBuffer = new ByteBuffer(1024 * 1024 * 10);
        try {
            byte[] tmp = new byte[4096];
            int length;
            while ((length = inputStream.read(tmp, 0, 4096)) > 0) {
                byteBuffer.append(tmp, 0, length);
            }
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }

        return byteBuffer.getData();
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

    static public void  makeSureDirExist(String dir) {
        File file  = new File(dir);
        if (file.exists()) {
            return;
        }
        file.mkdirs();
    }

    public static void saveFile(String path, InputStream inputStream) {

        File file = new File(path);
        if (file.exists()) {
            deleteDir(file);
        }
        file = new File(path);
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            byte[] bytes = new byte[4096];
            int length = 0;
            while ((length = inputStream.read(bytes)) > 0) {
                outputStream.write(bytes, 0, length);
            }
        } catch (IOException e) {
        }

    }

    public static void sendFile(String path, OutputStream outputStream) {

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
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }
    }
}
