package com.dtc.dingding.Model;

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
public class UserModel extends SuperModel {
    String unionid;
    String openId;
    String mobile;
    String department;
    String OnDuty;
    String OffDuty;
    String name;
    String OnResult;
    String OffResult;

}
