package demo;

import java.io.Serializable;

public class GirlVo implements Serializable {

    private static final long serialVersionUID = -8545755852948661858L;

    /**ID号*/
    String girlId;
    /**姓名*/
    String girlName;
    /**年龄*/
    long age;
    /**身高*/
    double height;
    /**罩杯*/
    String cupSize;
    public String getGirlId() {
        return girlId;
    }
    public void setGirlId(String girlId) {
        this.girlId = girlId;
    }
    public String getGirlName() {
        return girlName;
    }
    public void setGirlName(String girlName) {
        this.girlName = girlName;
    }
    public long getAge() {
        return age;
    }
    public void setAge(long age) {
        this.age = age;
    }
    public double getHeight() {
        return height;
    }
    public void setHeight(double height) {
        this.height = height;
    }
    public String getCupSize() {
        return cupSize;
    }
    public void setCupSize(String cupSize) {
        this.cupSize = cupSize;
    }

    @Override
    public String toString() {

        return "GirlVo [girlId=" + girlId
                +",girlName=" + girlName
                +",age=" + age
                +",height=" + height
                +",cupSize=" + cupSize
                +"]";
    }
}