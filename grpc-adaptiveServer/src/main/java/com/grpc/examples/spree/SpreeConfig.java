package com.grpc.examples.spree;

import java.text.SimpleDateFormat;

public final class SpreeConfig {

    public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI",
            "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    public final static String terminalPrefix = "Term-";

    public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public final static int configWhseCount = 1;
    public final static int configDistPerWhse = 10; // tpc-c std = 10
    public final static int configItemCount = 100000; // tpc-c std = 100,000
    public final static int configCustPerWhse = 30000; // spree users per stock location = 30,000

}