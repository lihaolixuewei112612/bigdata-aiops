package com.apex_ids.util.MyEnum;

public class Worth {
    public enum in{
        khh("string"),
        zzc("decimal(16,2)"),
        zfz("decimal(16,2)"),
        crje("decimal(16,2)"),
        qcje("decimal(16,2)"),
        zrzqsz("decimal(16,2)"),
        zczqsz("decimal(16,2)"),
        yk("decimal(16,2)"),
        qtsr("decimal(16,2)"),
        srfe("decimal(16,2)"),
        srjz("decimal(9,4)"),
        gdjz("decimal(9,4)"),
        cache("string");
        private String type;
        in(String type){
            this.type = type;
        }
        public String getType(){
            return this.type;
        }
        public int getIndex(){
            return this.ordinal();
        }
    }
    public enum out{
        khh("string"),
        dqfe("decimal(16,2)"),
        dqjz("decimal(9,4)"),
        gdjz("decimal(9,4)"),
        jzzzl("decimal(9,4)"),
        reload("int"),
        cache("string");
        private String type;
        out(String type){
            this.type = type;
        }
        public String getType(){
            return this.type;
        }
        public int getIndex(){
            return this.ordinal();
        }
    }


}
