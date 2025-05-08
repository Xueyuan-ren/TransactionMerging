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


package com.oltpbenchmark.benchmarks.spree.pojo;

import java.sql.Timestamp;

public class SpreeStocklocation {

    public int id;
    public String name;
    public Timestamp created_at;
    public Timestamp updated_at;
    public int def;
    public String address1;
    public String address2;
    public String city;
    public int state_id;
    public String state_name;
    public int country_id;
    public String zipcode;
    public String phone;
    public int active;
    public int backorderable_default;
    public int propagate_all_variants;
    public String admin_name;
    public Timestamp deleted_at;
   

    @Override
    public String toString() {
        return ("\n***************** SpreeStocklocation ********************"
                + "\n*         id = " + id
                + "\n*       name = " + name
                + "\n*       active = " + active
                + "\n**********************************************");
    }

}