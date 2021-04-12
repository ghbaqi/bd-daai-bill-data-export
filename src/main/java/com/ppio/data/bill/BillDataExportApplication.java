package com.ppio.data.bill;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@MapperScan(basePackages = {"com.ppio.data.bill.mysql.mapper"})
@SpringBootApplication
public class BillDataExportApplication {

    public static void main(String[] args) {
        SpringApplication.run(BillDataExportApplication.class, args);
    }

}
