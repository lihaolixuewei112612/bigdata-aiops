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
public class ThreadA extends Thread {

    private ThreadB b;

    public ThreadA(ThreadB b) {
        this.b = b;
    }

    @Override
    public void run() {
        try {
            synchronized (b) {
                System.out.println("begin A ThreadName="+
                        Thread.currentThread().getName()+" "
                        +System.currentTimeMillis());
                Thread.sleep(5000);
                System.out.println("end A ThreadName="+
                        Thread.currentThread().getName()+" "
                        +System.currentTimeMillis());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

