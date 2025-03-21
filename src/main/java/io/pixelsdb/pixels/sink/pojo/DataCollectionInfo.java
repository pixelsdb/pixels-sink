package io.pixelsdb.pixels.sink.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DataCollectionInfo implements Serializable {
    private String dataCollection;

    private Integer eventCount;
}
