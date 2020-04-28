package com.dtc.dingding.worker;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiAttendanceListRecordRequest;
import com.dingtalk.api.request.OapiDepartmentListRequest;
import com.dingtalk.api.request.OapiUserGetRequest;
import com.dingtalk.api.request.OapiUserSimplelistRequest;
import com.dingtalk.api.response.OapiAttendanceListRecordResponse;
import com.dingtalk.api.response.OapiDepartmentListResponse;
import com.dingtalk.api.response.OapiUserGetResponse;
import com.dingtalk.api.response.OapiUserSimplelistResponse;
import com.dtc.dingding.Model.UserModel;
import com.dtc.dingding.common.PropertiesConstants;
import com.google.gson.Gson;
import com.taobao.api.ApiException;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dtc.dingding.common.Untils.*;

/**
 * Created on 2020-02-28
 *
 * @author :hao.li
 */
public class Complete {
    private static final long PERIOD_DAY = 24 * 60 * 60 * 1000;
    private static final AtomicInteger count = new AtomicInteger();
    public ScheduledThreadPoolExecutor se = new ScheduledThreadPoolExecutor(1);

    public static void main(String[] args) throws IOException {
        new Complete();
    }
    public void fixedPeriodSchedule() {
        // 设定可以循环执行的runnable,初始延迟为0，这里设置的任务的间隔为4秒
        se.scheduleAtFixedRate(new FixedSchedule(), 0, 30, TimeUnit.MINUTES);
    }

    public Complete() {
        fixedPeriodSchedule();
        getSchedule();
    }

    class FixedSchedule implements Runnable {
        public void run() {

            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
            System.out.println("当前的系统时间为：" + sdf.format(new Date()));
            InputStream ips = Complete.class
                    .getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME);
            Properties props = new Properties();
            try {
                props.load(ips);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    ips.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                //获取部门列表
                DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_BUMEN_LIEBIAO).toString().trim());
                OapiDepartmentListRequest req = new OapiDepartmentListRequest();
                req.setHttpMethod("GET");
                OapiDepartmentListResponse rsp = client.execute(req, props.get(PropertiesConstants.DD_TOKEN).toString().trim());
                if (rsp.isSuccess()) {
                    String body = rsp.getBody();
                    JSONObject jsonObject = JSONObject.parseObject(body);
                    JSONArray department = (JSONArray) jsonObject.get("department");
                    for (int i = 0; i < department.size(); i++) {
                        JSONObject job = department.getJSONObject(i);
                        if ("true".equals(String.valueOf(job.get("createDeptGroup")))) {
                            String id = job.get("id").toString();
                            getDeparmentUser(Long.parseLong(id), props);
                        }
                    }
                }
            } catch (ApiException e) {
                e.printStackTrace();
            }
        }
    }
    public void getSchedule(){
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 1); //凌晨1点
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date date=calendar.getTime(); //第一次执行定时任务的时间
        //如果第一次执行定时任务的时间 小于当前的时间
        //此时要在 第一次执行定时任务的时间加一天，以便此任务在下个时间点执行。如果不加一天，任务会立即执行。
        if (date.before(new Date())) {
            date = this.addDay(date, 1);
        }
        Timer timer = new Timer();
       Task task = new Task();
        //安排指定的任务在指定的时间开始进行重复的固定延迟执行。
        timer.schedule(task,date,PERIOD_DAY);
    }
    // 增加或减少天数
    public Date addDay(Date date, int num) {
        Calendar startDT = Calendar.getInstance();
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, num);
        return startDT.getTime();
    }
    class Task extends TimerTask{
        public void run(){
            InputStream ips = Complete.class
                    .getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME);
            Properties props = new Properties();
            try {
                props.load(ips);
                JiaBan.getJiaBan(props);
                QingJia.getQingJia(props);
                KaoQin.getKaoQin(props);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ApiException e) {
                e.printStackTrace();
            } finally {
                try {
                    ips.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    /**
     * 获取部门用户
     */
    private static void getDeparmentUser(long departId, Properties props) {
        String name = null;
        DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_BUMEN_USER).toString().trim());
        OapiUserSimplelistRequest request = new OapiUserSimplelistRequest();
        request.setDepartmentId(departId);
        request.setHttpMethod("GET");
        try {
            OapiUserSimplelistResponse response = client.execute(request, props.get(PropertiesConstants.DD_TOKEN).toString().trim());
            String body1 = response.getBody();
            JSONObject jsonObject = JSONObject.parseObject(body1);
            if (!(jsonObject.getJSONArray("userlist").size() == 0)) {
                JSONArray userlist = jsonObject.getJSONArray("userlist");
                for (int i = 0; i < userlist.size(); i++) {
                    JSONObject job = userlist.getJSONObject(i);
                    int i1 = count.incrementAndGet();
                    name = job.get("name").toString();
                    if (name.contains("-")) {
                        name = name.split("-")[1];
                    }
                    String userid = job.get("userid").toString();
                    String userInfo = getUserInfo(userid, props);
                    Gson gson = new Gson();
                    UserModel userModel = gson.fromJson(userInfo, UserModel.class);
                    writeMysql("SC_KQ_USER", userModel, props);
                }
            }

        } catch (ApiException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取用户详情
     */
    private static String getUserInfo(String userid, Properties props) {
        String body;
        String jsonString = null;
        DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_USER_INFO).toString().trim());
        OapiUserGetRequest request = new OapiUserGetRequest();
        request.setUserid(userid);
        request.setHttpMethod("GET");
        try {
            OapiUserGetResponse response = client.execute(request, props.get(PropertiesConstants.DD_TOKEN).toString().trim());
            Map<String, String> record = getRecord(userid, props);
            body = response.getBody();
            JSONObject jsonObject = JSONObject.parseObject(body);
            String department = jsonObject.get("department").toString();
            if (department.contains("[") && department.contains("]")) {
                String[] department1 = department.split("\\[")[1].split("]");
                record.put("department", department1[0]);
            } else {
                record.put("department", jsonObject.get("department").toString());
            }
            record.put("unionid", jsonObject.get("unionid").toString());
            record.put("openId", jsonObject.get("openId").toString());
            record.put("userid", jsonObject.get("userid").toString());
            record.put("mobile", jsonObject.get("mobile").toString());
            record.put("name", jsonObject.get("name").toString());
            jsonString = JSON.toJSONString(record);
        } catch (ApiException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    /**
     * 得到打卡时间
     */
    private static Map<String, String> getRecord(String userID, Properties props) throws ApiException {
        DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_DAKA).toString().trim());
        OapiAttendanceListRecordRequest request = new OapiAttendanceListRecordRequest();
        Map<String, String> time = getTime();
        String stattime = time.get("starttime");
        String endtime = time.get("endtime");
        String start = timeStamp2Date(stattime, "yyyy-MM-dd HH:mm:ss");
        String end = timeStamp2Date(endtime, "yyyy-MM-dd HH:mm:ss");
        request.setCheckDateFrom(start);
        request.setCheckDateTo(end);
        request.setUserIds(Arrays.asList(userID));
        OapiAttendanceListRecordResponse execute = client.execute(request, props.get(PropertiesConstants.DD_TOKEN).toString().trim());
        String body = execute.getBody();

        JSONObject jsonObject = JSONObject.parseObject(body);
        if (jsonObject == null) {
            return new HashMap<>();
        }
        JSONArray recoder = (JSONArray) jsonObject.get("recordresult");
        Map<String, String> map = new HashMap();
        try {
            if (recoder.size() == 2) {
                for (int i = 0; i < recoder.size(); i++) {
                    JSONObject job = recoder.getJSONObject(i);
                    String id = job.get("checkType").toString();
                    map.put(id, job.get("userCheckTime").toString());
                    map.put("OnResult", job.get("timeResult").toString());
                    map.put("OffResult", job.get("timeResult").toString());

                }
            } else if (recoder.size() == 1) {
                JSONObject job = recoder.getJSONObject(0);
                String id = job.get("checkType").toString();
                if ("OnDuty".equals(id)) {
                    map.put("OnDuty", job.get("userCheckTime").toString());
                    map.put("OffDuty", "null");
                } else if ("OffDuty".equals(id)) {
                    map.put("OffDuty", job.get("userCheckTime").toString());
                    map.put("OnDuty", "null");
                }
            } else {
                map.put("OffDuty", "null");
                map.put("OnDuty", "null");
                map.put("OnResult", "null");
                map.put("OffResult", "null");


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

}



