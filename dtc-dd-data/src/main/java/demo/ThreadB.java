package demo;

/**
 * @Author : lihao
 * Created on : 2020-07-15
 * @Description : TODO描述类作用
 */
/**
 * @author: xianzhixianzhixian
 * @date: 2019-01-07 21:34
 */
public class ThreadB extends Thread {
    @Override
    synchronized public void run() {
        try {
            System.out.println("begin B ThreadName="+
                    Thread.currentThread().getName()+" "
                    +System.currentTimeMillis());
            Thread.sleep(5000);
            System.out.println("end B ThreadName="+
                    Thread.currentThread().getName()+" "
                    +System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

