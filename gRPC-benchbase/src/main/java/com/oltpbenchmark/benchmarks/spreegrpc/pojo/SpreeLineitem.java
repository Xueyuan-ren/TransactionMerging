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


package com.oltpbenchmark.benchmarks.spreegrpc.pojo;

import java.sql.Timestamp;

public class SpreeLineitem {

    public int id;
    public int variant_id;
    public int order_id;
    public int quantity;
    public float price;
    public Timestamp created_at;
    public Timestamp updated_at;
    public String currency;
    public float cost_price;
    public int tax_category_id;
    public float adjustment_total;
    public float additional_tax_total;
    public float promo_total;
    public float included_tax_total;
    public float pre_tax_amount;
    public float taxable_adjustment_total;
    public float non_taxable_adjustment_total;
    public String public_metadata;
    public String private_metadata;

    @Override
    public String toString() {
        return ("\n***************** SpreeLineitem ********************"
                + "\n*         id = " + id
                + "\n*       variant_id = " + variant_id
                + "\n*       order_id = " + order_id
                + "\n*       quantity = " + quantity
                + "\n*       price = " + price
                + "\n**********************************************");
    }

}