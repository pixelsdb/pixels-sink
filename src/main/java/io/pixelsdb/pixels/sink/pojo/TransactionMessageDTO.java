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

}
