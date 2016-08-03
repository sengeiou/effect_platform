package com.ali.lz.effect.hadooputils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 操作HDFS文件、目录的工具方法
 * @author feiqiong.dpf
 *
 */
public class HdfsUtils {

    /**
     * 删除这些目录
     * 
     * @param paths
     * @throws IOException
     */
    public static void rmHdfsDir(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
        fs.close();
    }

    public static void mvResultDir(String sourcePath, String targetPath, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);

            if (!fs.exists(new Path(targetPath))) {
                if (!fs.mkdirs(new Path(targetPath))) {
                    System.out.println("Unable to make directory: " + targetPath);
                }
            }

            FileStatus[] fileStatusArray = fs.listStatus(new Path(sourcePath));
            if (fileStatusArray == null)
                return;
            for (FileStatus fileStatus : fileStatusArray) {
                if (fs.isFile(fileStatus.getPath())) {
                    // DO SOMETHING YOU WANT
                    // Ex. System.out.println()
                } else if (fs.isDirectory(fileStatus.getPath())) {
                    mvResultDirs(fileStatus.getPath(), new Path(targetPath + "/" + fileStatus.getPath().getName()),
                            conf);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 转移src目录下的所有内容到dist目录下
     * 
     * @param src
     * @param dist
     */
    public static void mvResultDirs(Path sourcePath, Path targetPath, Configuration conf) {
        try {
            FileSystem fs = sourcePath.getFileSystem(conf);

            fs.delete(targetPath, true);
            if (fs.exists(sourcePath)) {
                if (!fs.rename(sourcePath, targetPath)) {
                    System.out.println("Unable to rename: " + sourcePath + " to: " + targetPath);
                }
            } else if (!fs.mkdirs(targetPath)) {
                System.out.println("Unable to make directory: " + targetPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
