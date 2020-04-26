package com.dtc.dingding;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiAttendanceListRecordRequest;
import com.dingtalk.api.response.OapiAttendanceListRecordResponse;
import com.taobao.api.ApiException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2020-03-06
 *
 * @author :hao.li
 */
public class recode {
    public static void main(String[] args) throws ApiException {
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/attendance/listRecord");
        OapiAttendanceListRecordRequest request = new OapiAttendanceListRecordRequest();
        request.setCheckDateFrom("2020-03-16 00:00:00");
        request.setCheckDateTo("2020-03-17 00:00:00");
        request.setUserIds(Arrays.asList("2100455427842711"));
        OapiAttendanceListRecordResponse execute = client.execute(request, "7c61e56799d93ff5833c0bc2a2cfbe48");
        String body = execute.getBody();
        System.out.println(body);
        JSONObject jsonObject = JSONObject.parseObject(body);
        JSONArray recoder = (JSONArray) jsonObject.get("recordresult");
        Map<String, String> map = new HashMap();
        String  userid=null;
        if(recoder.size()==2) {
            for (int i = 0; i < recoder.size(); i++) {
                JSONObject job = recoder.getJSONObject(i);
                String id = job.get("checkType").toString();
                userid = job.get("userId").toString();
                map.put("user_ID", userid);
                map.put(id, job.get("userCheckTime").toString());
            }
        }else if(recoder.size()==1){
            JSONObject job = recoder.getJSONObject(0);
            String id = job.get("checkType").toString();
            if ("OnDuty".equals(id)) {
                userid = job.get("userId").toString();
                map.put("OnDuty", job.get("userCheckTime").toString());
                map.put("user_ID", userid);
                map.put("OffDuty","null");
            }else if ("OffDuty".equals(id)) {
                userid = job.get("userId").toString();
                map.put("OffDuty", job.get("userCheckTime").toString());
                map.put("user_ID", userid);
                map.put("OnDuty","null");
            }
        }else{
            map.put("user_ID", "null");
            map.put("OffDuty","null");
            map.put("OnDuty","null");
        }
        String jsonString = JSON.toJSONString(map);
        System.out.println(jsonString);
    }
}
