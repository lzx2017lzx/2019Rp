package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Lower extends UDF {
    public String evaluate(final String a){
        if(a == null){
            return null;
        }
        return a.toLowerCase();
    }

}
