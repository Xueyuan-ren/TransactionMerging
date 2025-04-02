/*
 * Copyright 2020 by OLTPBenchmark Project
 *
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
 *
 */


package com.oltpbenchmark.benchmarks.spreegrpc;

import com.oltpbenchmark.util.RandomGenerator;
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
