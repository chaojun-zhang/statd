package io.statd.server.config;

import io.statd.core.storage.config.Storage;
import lombok.Data;

@Data
public class StorageConfig {
    private Storage storage;
    //可以配合IAM做resource权限控制
    private IamConfig iam;
    private String render;
    private boolean array;
}
