//package com.dtc.dingding;
//
//import java.io.ByteArrayInputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.util.Arrays;
//
///**
// * @Author : lihao
// * Created on : 2020-07-03
// * @Description : TODO描述类作用
// */
//public class MagicParser {
//
//    public static final byte[] MAGIC_HEADER = new byte[]{(byte) 0xfe, (byte) 0x62, (byte) 0x69, (byte) 0x6e};
//
//    public static void main(String[] args)throws Exception {
//        String filePath = "/Users/lixuewei/workspace/DTC/workspace/test/dtc_worker/conf/mysql-bin.000002";
//        File binlogFile = new File(filePath);
//        ByteArrayInputStream inputStream = new ByteArrayInputStream(new FileInputStream(binlogFile));
//
//        byte[] magicHeader = inputStream.read(4);
//        System.out.println("魔数\\xfe\\x62\\x69\\x6e是否正确:"+ Arrays.equals(MAGIC_HEADER, magicHeader));
//    }
//}
