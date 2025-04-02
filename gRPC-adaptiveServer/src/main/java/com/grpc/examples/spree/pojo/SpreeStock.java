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


package com.grpc.examples.spree.pojo;

import java.sql.Timestamp;

public class SpreeStock {

    public int id;
    public int stock_location_id;
    public int variant_id;
    public int count_on_hand;
    public Timestamp created_at;
    public Timestamp updated_at;
    public int backorderable;
    public Timestamp deleted_at;
    public String public_metadata;
    public String private_metadata;

    @Override
    public String toString() {
        return ("\n***************** SpreeStock ********************"
                + "\n*         id = " + id
                + "\n*       stock_location_id = " + stock_location_id
                + "\n*       variant_id = " + variant_id
                + "\n*       count_on_hand = " + count_on_hand
                + "\n**********************************************");
    }

}