package io.pixelsdb.pixels.sink.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DataCollectionInfo implements Serializable {
    /**
     * 数据集合名称（如：public.nation）
     */
    private String dataCollection;

    /**
     * 事件数量
     */
    private Integer eventCount;
}
