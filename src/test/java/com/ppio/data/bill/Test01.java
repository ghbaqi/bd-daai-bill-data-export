package com.ppio.data.bill;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@SpringBootTest
@Slf4j
@RunWith(SpringRunner.class)
public class Test01 {

    /**
     *
     */


    @Test
    public void testSql() {

        Account account = new AliyunAccount(Config.ACCESS_ID, Config.ACCESS_KEY);
        Odps odps = new Odps(account);
        odps.setEndpoint(Config.END_POINT);
        odps.setDefaultProject(Config.PROJECT_NAME);
        Instance i;

        String getDistinctMachineidSql = String.format(Config.GET_DISTINCT_MACHINEID_SQL_TEMPLATE, 20200901);

        log.info(getDistinctMachineidSql);

        try {
            i = SQLTask.run(odps, getDistinctMachineidSql);
            i.waitForSuccess();
            List<Record> records = SQLTask.getResult(i);

            log.info("data size = " + records.size());
            for (Record r : records) {
                log.info(r.getString(0));
            }
        } catch (OdpsException e) {
            e.printStackTrace();
        }
    }

}
