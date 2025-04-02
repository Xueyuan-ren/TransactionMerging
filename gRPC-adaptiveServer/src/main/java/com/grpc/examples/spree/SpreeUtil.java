package com.grpc.examples.spree;

import com.grpc.examples.util.RandomGenerator;
import java.util.Random;

public class SpreeUtil {

    private static final RandomGenerator ran = new RandomGenerator(0);

    public static String randomStr(int strLen) {
        if (strLen > 1) {
            return ran.astring(strLen - 1, strLen - 1);
        } else {
            return "";
        }
    }

    public static String randomNStr(int stringLength) {
        if (stringLength > 0) {
            return ran.nstring(stringLength, stringLength);
        } else {
            return "";
        }
    }

    public static String formattedDouble(double d) {
        String dS = "" + d;
        return dS.length() > 6 ? dS.substring(0, 6) : dS;
    }

    public static int randomNumber(int min, int max, Random r) {
        return (int) (r.nextDouble() * (max - min + 1) + min);
    }

    public static int nonUniformRandom(int A, int C, int min, int max, Random r) {
        return (((randomNumber(0, A, r) | randomNumber(min, max, r)) + C) % (max
                - min + 1))
                + min;
    }

}