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
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TimesConstats {
    private int one;
    private int two;
    private int three;
    private int four;

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
}
