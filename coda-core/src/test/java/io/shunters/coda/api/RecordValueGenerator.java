package io.shunters.coda.api;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

/**
 * Created by mykidong on 2017-08-31.
 */
public class RecordValueGenerator {

    /**
     * TODO: if it is used, snappy uncompression error occurred!!!!!!
     *
     * @param size
     * @return
     */
    private static byte[] getRandomByteArray(int size){
        byte[] result= new byte[size];
        Random random= new Random();
        random.nextBytes(result);

        return result;
    }


    public static byte[] generateBytesWithString(int size){
        String string = "a";
        byte[] result = new byte[size];
        System.arraycopy(string.getBytes(), 0, result, size - string.length(), string.length());

        return result;
    }

}
