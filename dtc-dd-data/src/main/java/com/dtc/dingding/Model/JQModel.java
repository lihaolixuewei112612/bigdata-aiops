package com.dtc.dingding.Model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-03-09
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JQModel extends SuperModel{
    @SerializedName(value="qingjia_time",alternate = "jiaban_time")
    String time;
    @SerializedName("start_time")
    String starttime;
    @SerializedName("end_time")
    String endtime;

}
