package com.lzx.groupcomparable2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupCompare extends WritableComparator {
    protected GroupCompare(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean)a;
        OrderBean ob = (OrderBean)b;

        if(oa.getId() > ob.getId()){
            return 1;
        }else if(oa.getId() < ob.getId()){
            return -1;
        }else{
            return oa.getPrice() > ob.getPrice() ? -1 : 1;
        }
    }
}
