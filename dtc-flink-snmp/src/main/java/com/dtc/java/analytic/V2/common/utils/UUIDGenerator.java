package com.dtc.java.analytic.V2.common.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @Author : lihao
 * Created on : 2020-04-07
 * @Description : TODO描述类作用
 */
public class UUIDGenerator {

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random rng = new SecureRandom();

    private static char randomChar(){
        return ALPHABET.charAt(rng.nextInt(ALPHABET.length()));
    }

    public static String uuid(int length, int spacing, char spacerChar){
        StringBuilder sb = new StringBuilder();
        int spacer = 0;
        while(length > 0){
            if(spacer == spacing){
                sb.append(spacerChar);
                spacer = 0;
            }
            length--;
            spacer++;
            sb.append(randomChar());
        }
        return sb.toString();
    }

    public static String generateUserCode() {
        return uuid(6, 10, ' ');
    }

//    public static void main(String[] args) {
//        System.out.println(generateUserCode());
//    }
}
