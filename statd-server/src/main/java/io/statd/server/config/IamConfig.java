package io.statd.server.config;

import lombok.Data;

import java.util.List;

@Data
public class IamConfig {
    //当前Iam配置属于哪个服务
    private String service;

    //受控制的resourceTypes,见IAM里面对Resource的配置
    //为空代表不做控制
    private List<String> resourceTypes;
}
