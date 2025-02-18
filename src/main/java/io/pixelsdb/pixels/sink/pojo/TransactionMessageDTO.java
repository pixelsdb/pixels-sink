package io.pixelsdb.pixels.sink.pojo;

import io.pixelsdb.pixels.sink.pojo.enums.TransactionStatusEnum;
import lombok.Data;
import java.io.Serializable;
import java.util.List;

@Data
public class TransactionMessageDTO implements Serializable {
    private static final long serialVersionUID = 1L;

    private TransactionStatusEnum status;
    private String id; // transaction id
    private Integer eventCount;
    private List<DataCollectionInfo> dataCollections;

    private Long tsMs;

    @Data
    public static class DataCollectionInfo implements Serializable {
        /**
         * 数据集合名称（如：public.nation）
         */
        private String dataCollection;

        /**
         * 事件数量
         */
        private Integer eventCount;
    }
}
