package com.ppio.data.bill.mongdb;

import lombok.Data;

@Data
public class BwEntity {

    private String timeId;

    private double bwupload;



    public BwEntity() {
    }

    public BwEntity(String timeId, double bwupload) {
        this.timeId = timeId;
        this.bwupload = bwupload;
    }
}
