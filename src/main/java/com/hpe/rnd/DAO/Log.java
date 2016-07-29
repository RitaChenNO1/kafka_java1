package com.hpe.rnd.DAO;

/**
 * Created by chzhenzh on 7/19/2016.
 */
public class Log {
    public static void Info(String message) {
        System.out.println("INFO:" + message);
    }

    public static void Error(String message) {
        System.out.println("Error:" + message);
    }
}
