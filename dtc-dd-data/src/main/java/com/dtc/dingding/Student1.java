package com.dtc.dingding;

public class Student1 implements Comparable<Student1> {

    private String name;
    private int age;

    public Student1() {
    }

    public Student1(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public int compareTo(Student1 s) {
        int num = this.age - s.age;
        int num1 = (num == 0 ? this.name.compareTo(s.name) : num);
        return num1;
    }
}
