package com.ppio.data.bill.mongdb;

import lombok.Data;
import lombok.ToString;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.List;


@ToString
@Data
@Document(collection = "dcache_202103_bsbwupload")
public class TaskEntity implements Serializable {

    public String date;

    public long timestamp;

    public String business = "daai";

    public String deviceId;

    public String dcacheId;

    public List<Double> bandwidthSeries;

    public List<BwEntity> series;

}
