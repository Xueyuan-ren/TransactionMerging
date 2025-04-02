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

public class SpreeProduct {

    public int id;
    public String name;
    public String description;
    public Timestamp available_on;
    public Timestamp deleted_at;
    public String slug;
    public String meta_description;
    public String meta_keywords;
    public int tax_category_id;
    public int shipping_category_id;
    public Timestamp created_at;
    public Timestamp updated_at;
    public int promotionable;
    public String meta_title;
    public Timestamp discontinue_on;
    public String public_metadata;
    public String private_metadata;
    public String status;
    public Timestamp make_active_at;

    @Override
    public String toString() {
        return ("\n***************** SpreeProduct ********************"
                + "\n*         id = " + id
                + "\n*       name = " + name
                + "\n**********************************************");
    }

}