package demo;

/**
 * @Author : lihao
 * Created on : 2020-07-15
 * @Description : TODO描述类作用
 */

public class Run {
    public static void main(String[] args) {
        try {
            ThreadB b = new ThreadB();
            ThreadA a = new ThreadA(b);
            a.start();
            b.start();
            b.join(2000);
            System.out.println("main end "+System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

