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

public class SpreeUser {

    public int id;
    public String encrypted_password;
    public String password_salt;
    public String email;
    public String remember_token;
    public String persistence_token;
    public String reset_password_token;
    public String perishable_token;
    public int sign_in_count;
    public int failed_attempts;
    public Timestamp last_request_at;
    public Timestamp current_sign_in_at;
    public Timestamp last_sign_in_at;
    public String current_sign_in_ip;
    public String last_sign_in_ip;
    public String login;
    public int ship_address_id;
    public int bill_address_id;
    public String authentication_token;
    public String unlock_token;
    public Timestamp locked_at;
    public Timestamp reset_password_sent_at;
    public Timestamp created_at;
    public Timestamp updated_at;
    public String public_metadata;
    public String private_metadata;
    public String first_name;
    public String last_name;
    public String selected_locale;
    public String spree_api_key;
    public Timestamp remember_created_at;
    public Timestamp deleted_at;
    public String confirmation_token;
    public Timestamp confirmed_at;
    public Timestamp confirmation_sent_at;

    @Override
    public String toString() {
        return ("\n***************** SpreeUser *******************"
                + "\n*             id = "
                + id
                + "\n*          email = "
                + email
                + "\n**********************************************");
    }

}
