package demo;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Author : lihao
 * Created on : 2020-07-15
 * @Description : TODO描述类作用
 */
public class JoinTester01 implements Runnable {

    private String name;

    public JoinTester01(String name) {
        this.name = name;
    }

    public void run() {
        System.out.printf("%s begins: %s\n", name, new Date());
        try {
            TimeUnit.SECONDS.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.printf("%s has finished: %s\n", name, new Date());
    }

    public static void main(String[] args) {
        Thread thread1 = new Thread(new JoinTester01("One"));
        Thread thread2 = new Thread(new JoinTester01("Two"));
        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Main thread is finished");
    }
}

