package eventtwo;

import com.google.common.eventbus.Subscribe;

/**
 * Copyright @ 2018 lihao.com
 * All right reserved.
 *
 * @author Li Hao
 * @since 2018/11/9  18:56
 */
public class DataObserver1 {
    @Subscribe
    public void func(String msg) {
        System.out.println("String msg: " + msg);
    }
}