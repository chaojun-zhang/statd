package io.statd.core.storage.config;

public interface SourceStorage extends SingleStorage {

    /**
     * 底层存储的时间字段
     *
     * @return
     */
    String getEventTimeField();

    /**
     * 最多允许7天的查询
     *
     * @return
     */
    int getMaxQueryDays();

}
