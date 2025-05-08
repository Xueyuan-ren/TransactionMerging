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

public class SpreeOrder {

    public int id;
    public String number;
    public float item_total;
    public float total;
    public String state;
    public float adjustment_total;
    public int user_id;
    public Timestamp completed_at;
    public int bill_address_id;
    public int ship_address_id;
    public float payment_total;
    public String shipment_state;
    public String payment_state;
    public String email;
    public String special_instructions;
    public Timestamp created_at;
    public Timestamp updated_at;
    public String currency;
    public String last_ip_address;
    public int created_by_id;
    public float shipment_total;
    public float additional_tax_total;
    public float promo_total;
    public String channel;
    public float included_tax_total;
    public int item_count;
    public int approver_id;
    public Timestamp approved_at;
    public int confirmation_delivered;
    public int considered_risky;
    public String token;
    public Timestamp canceled_at;
    public int canceler_id;
    public int store_id;
    public int state_lock_version;
    public float taxable_adjustment_total;
    public float non_taxable_adjustment_total;
    public int store_owner_notification_delivered;
    public String public_metadata;
    public String private_metadata;
    public String internal_note;

    @Override
    public String toString() {
        return ("\n***************** SpreeOrder ********************"
                + "\n*         id = " + id
                + "\n*       number = " + number
                + "\n*       total = " + total
                + "\n*       state = " + state
                + "\n*       email = " + email
                + "\n**********************************************");
    }

}