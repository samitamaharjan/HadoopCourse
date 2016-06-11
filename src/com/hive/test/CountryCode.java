package com.hive.test;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class CountryCode extends UDF {
    private Text result = new Text();

    public Text evaluate(String phone_number) {

        if (phone_number != null) {
            if (!phone_number.substring(0, 2).equalsIgnoreCase("+1")) {
                phone_number = "+1" + phone_number;
            }
        }

        result.set(phone_number);

        return result;

    }
}


