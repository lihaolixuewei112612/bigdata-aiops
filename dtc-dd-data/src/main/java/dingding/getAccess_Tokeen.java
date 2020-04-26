package dingding;

import com.taobao.api.ApiException;

/**
 * Created on 2020-02-28
 *
 * @author :hao.li
 */
public class getAccess_Tokeen {
    public static void main(String[] args) throws ApiException {
        TokenThread tt = new TokenThread();
        Thread t1 = new Thread(tt);
        t1.setName("获取钉钉token线程");
        t1.start();
      //  qingjia.info();
       // jiaban.info();
       // kaoqin.info();


    }
}
