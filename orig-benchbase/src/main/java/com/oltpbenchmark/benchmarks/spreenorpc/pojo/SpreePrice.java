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


package com.oltpbenchmark.benchmarks.spreenorpc.pojo;

import java.sql.Timestamp;

public class SpreePrice {

    public int id;
    public int variant_id;
    public double amount;
    public String currency;
    public Timestamp deleted_at;
    public Timestamp created_at;
    public Timestamp updated_at;
    public double compare_at_amount;


    @Override
    public String toString() {
        return ("\n***************** SpreePrice ********************"
                + "\n*         id = " + id
                + "\n*       variant_id = " + variant_id
                + "\n*       amount = " + amount
                + "\n**********************************************");
    }

}