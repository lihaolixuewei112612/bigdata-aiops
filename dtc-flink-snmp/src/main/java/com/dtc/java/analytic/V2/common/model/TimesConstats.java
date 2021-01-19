package com.dtc.java.analytic.V2.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : lihao
 * Created on : 2020-05-18
 * @Description : TODO描述类作用
 */

public class TimesConstats {
    private int one;
    private int two;
    private int three;
    private int four;

    public TimesConstats() {
        this(new Builder());
    }
    public TimesConstats(Builder builder){
        this.one = builder.one;
        this.two = builder.two;
        this.three = builder.three;
        this.four = builder.four;
    }
    public int getOne() {
        return one;
    }

    public void setOne(int one) {
        this.one = one;
    }

    public int getTwo() {
        return two;
    }

    public void setTwo(int two) {
        this.two = two;
    }

    public int getThree() {
        return three;
    }

    public void setThree(int three) {
        this.three = three;
    }

    public int getFour() {
        return four;
    }

    public void setFour(int four) {
        this.four = four;
    }
    public Builder newBuilder() {
        return new Builder(this);
    }

    public static class Builder{
        private int one;
        private int two;
        private int three;
        private int four;
        public Builder(){
            one =one;
            two=two;
            three=three;
            four=four;
        }
        Builder(TimesConstats tc){
            this.one = tc.one;
            this.two= tc.two;
            this.three=tc.three;
            this.four=tc.four;
        }
        public Builder one(int one){
            one = one;
            return this;
        }
        public Builder two(int two){
            two = two;
            return this;
        }
        public Builder three(int three){
            three = three;
            return this;
        }
        public Builder four(int four){
            four = four;
            return this;
        }
        public TimesConstats build(){
            return new TimesConstats(this);
        }
    }
}
