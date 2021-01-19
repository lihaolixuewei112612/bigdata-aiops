package com.file.modify;

import java.io.File;
import java.sql.Timestamp;

public class FileInfo {
    public static void main(String[] args) {
        File f= new File("D:\\workespace\\test.txt");
        long size = f.length();
        long modify = f.lastModified();
        System.out.println(new Timestamp(modify).toString());
    }
}
