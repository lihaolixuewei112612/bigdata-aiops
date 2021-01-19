package com.dtc.dingding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class test {
    public static void main(String[] args) {
        List<Student1> list1 = new ArrayList<Student1>();
        list1.add(new Student1("林青霞", 27));
        list1.add(new Student1("风清扬", 30));
        list1.add(new Student1("刘晓曲", 28));
        list1.add(new Student1("武鑫", 29));
        list1.add(new Student1("林青霞", 27));

        Collections.sort(list1);
        for (Student1 s : list1) {
            System.out.println(s.getName() + "---" + s.getAge());
        }
    }
}
