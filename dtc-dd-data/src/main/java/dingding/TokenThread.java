package dingding;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.request.OapiGettokenRequest;
import com.dingtalk.api.response.OapiGettokenResponse;
import com.taobao.api.ApiException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created on 2020-02-28
 *
 * @author :hao.li
 */
@Slf4j
public class TokenThread implements Runnable {
    // 第三方用户唯一凭证
    public static String appid = "dingmobls5odfginbbhs";
    // 第三方用户唯一凭证密钥
    public static String appsecret = "d4H-UjqGtqHF5Dnu09wvgwRgxYqwgV1WlFAl-yh0OmrmWXl5ZpQn6f5q0ttr0cfI";
    public static String accessToken = null;
    private static final Logger LOG = LoggerFactory.getLogger(TokenThread.class);

    @Override
    public void run() {
        while (true) {
            try {
                DefaultDingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/gettoken");
                OapiGettokenRequest request = new OapiGettokenRequest();
                request.setAppkey(appid);
                request.setAppsecret(appsecret);
                request.setHttpMethod("GET");
                OapiGettokenResponse response = client.execute(request);
                if ("ok" != response.getErrmsg()) {
                  //  log.info("获取access_token成功，有效时长{}秒 token:{}", response.getExpiresIn(), response.getAccessToken());
                    LOG.info("获取access_token成功，有效时长{}秒 token:{}", response.getExpiresIn(), response.getAccessToken());
                    // 休眠3000秒
                    String Url = "jdbc:mysql://10.3.7.231:3306/real_time_alarm_data?useUnicode=true&characterEncoding=UTF-8";
                    String JDBCDriver = "com.mysql.jdbc.Driver";
                    long l = System.currentTimeMillis();
                    String sql = "replace into token_data(token,init_time) values (?,?)";
                    Connection con = null;
                    try {
                        //向DriverManager注册自己
                        Class.forName(JDBCDriver);
                        //与数据库建立连接
                        con = DriverManager.getConnection(Url, "root", "DTCserver2019!");
                        //用来执行SQL语句查询，对sql语句进行预编译处理
                        PreparedStatement pst = con.prepareStatement(sql);
                        pst.setString(1, response.getAccessToken());
                        pst.setString(2,String.valueOf(l));
                        pst.executeUpdate();
                       // log.info("成功将token写入到mysql中!!!");
                        LOG.info("成功将token写入到mysql中!!!");
                    } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        if (con != null) {
                            try {
                                con.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                    Thread.sleep(3000 * 1000);
                } else {
                    // 如果access_token为null，60秒后再获取
                    Thread.sleep(60 * 1000);
                }
            } catch (InterruptedException | ApiException e) {
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e1) {
                }
            }
        }
    }

}
