package com.dtc.dingding;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiDepartmentListRequest;
import com.dingtalk.api.response.OapiDepartmentListResponse;
import com.taobao.api.ApiException;

/**
 * Created on 2020-02-28
 *
 * @author :hao.li
 */
public class test {
    public static void main(String[] args) {
        try {
            DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/department/list");
            OapiDepartmentListRequest req = new OapiDepartmentListRequest();
            req.setHttpMethod("GET");
            OapiDepartmentListResponse rsp = client.execute(req, "16d51110c88b38a9bf568ac2b27e4640");
            System.out.println(rsp.getBody());
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }
}

