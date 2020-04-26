package com.dtc.dingding.worker;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiProcessinstanceGetRequest;
import com.dingtalk.api.request.OapiProcessinstanceListidsRequest;
import com.dingtalk.api.response.OapiProcessinstanceGetResponse;
import com.dingtalk.api.response.OapiProcessinstanceListidsResponse;
import com.dtc.dingding.Model.JQModel;
import com.dtc.dingding.common.PropertiesConstants;
import com.google.gson.Gson;
import com.taobao.api.ApiException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.dtc.dingding.common.Untils.getTime;
import static com.dtc.dingding.common.Untils.writeMysql;

/**
 * @Author : lihao
 * Created on : 2020-04-26
 * @Description : TODO描述类作用
 */
public class JiaBan {
    public static void getJiaBan(Properties props) throws ApiException {
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/listids");
        OapiProcessinstanceListidsRequest req = new OapiProcessinstanceListidsRequest();
        req.setProcessCode(props.get(PropertiesConstants.DD_JIABAN).toString());
        Map<String, String> time = getTime();
        Long starttime = Long.parseLong(time.get("starttime"));
        Long endtime = Long.parseLong(time.get("endtime"));
        req.setStartTime(starttime);
        req.setEndTime(endtime);
        OapiProcessinstanceListidsResponse response = client.execute(req, props.get(PropertiesConstants.DD_TOKEN).toString());
        String str = response.getBody();
        JSONObject jsonObject = JSONObject.parseObject(str);
        JSONArray result = jsonObject.getJSONObject("result").getJSONArray("list");
        for (int i = 0; i < result.size(); i++) {
            String str1 = result.getString(i);
            DingTalkClient client1 = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/get");
            OapiProcessinstanceGetRequest request = new OapiProcessinstanceGetRequest();
            request.setProcessInstanceId(str1);
            OapiProcessinstanceGetResponse response1 = client1.execute(request, props.get(PropertiesConstants.DD_TOKEN).toString());
            String body = response1.getBody();
            JSONObject jsonObject1 = JSONObject.parseObject(body);
            String status = jsonObject1.getJSONObject("process_instance").getString("status");
            String task_result = jsonObject1.getJSONObject("process_instance").getString("result");
            Map<String, String> map = new HashMap<>();
            if ("COMPLETED".equals(status) && "agree".equals(task_result)) {
                JSONArray operation_records = jsonObject1.getJSONObject("process_instance").getJSONArray("operation_records");
                for (int j = 0; j < operation_records.size(); j++) {
                    JSONObject jsonObject2 = operation_records.getJSONObject(j);
                    System.out.println(jsonObject2);
                    String operation_type = jsonObject2.getString("operation_type");
                    if ("START_PROCESS_INSTANCE".equals(operation_type)) {
                        String userid = jsonObject2.getString("userid");
                        map.put("userid", userid);
                        JSONArray form_component_values = jsonObject1.getJSONObject("process_instance").getJSONArray("form_component_values");
                        for (int m = 0; m < form_component_values.size(); m++) {
                            JSONObject jsonObject3 = form_component_values.getJSONObject(m);
                            if ("加班时长".equals(jsonObject3.getString("name"))) {
                                String duration = jsonObject3.getString("value");
                                if (duration.contains("小时")) {
                                    String duration1 = duration.replace("小时", "");
                                    map.put("jiaban_time", duration1);
                                } else {
                                    map.put("jiaban_time", duration);
                                }
                            }
                            if ("开始时间-结束时间".equals(jsonObject3.getString("id"))) {
                                String value = jsonObject3.getString("value");
                                if (value.startsWith("[") && value.endsWith("]")) {
                                    String[] split = value.split("\\[")[1].split("]");
                                    String start_time = split[0].split(",")[0].replace("\"", "");
                                    String end_time = split[0].split(",")[1].replace("\"", "");
                                    map.put("start_time", start_time);
                                    map.put("end_time", end_time);
                                }
                            }
                        }
                    }
                }
                String jsonString = JSON.toJSONString(map);
                Gson gson = new Gson();
                JQModel model = gson.fromJson(jsonString, JQModel.class);
                writeMysql("SC_KQ_JB", model,props);
            }
        }
    }
}
