package ltb;

import org.apache.log4j.Logger;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

/**
 * Copyright (c) 2014 HappyGears, Inc
 * author: vadim
 * Date: 10/11/14
 */
public class TestLogger extends TestListenerAdapter {
    private static final Logger log = Logger.getLogger(new Throwable().getStackTrace()[0].getClassName());

    private int m_count = 0;

    @Override
    public void onTestStart(ITestResult tr) {
        log(tr, "start");
    }

    @Override
    public void onTestFailure(ITestResult tr) {
        log(tr, "failed");
    }

    @Override
    public void onTestSkipped(ITestResult tr) {
        log(tr, "skipped");
    }

    @Override
    public void onTestSuccess(ITestResult tr) {
        log(tr, "success");
    }

    private void log(ITestResult tr, String string) {
        String name = Thread.currentThread().getName();
        long id = Thread.currentThread().getId();
        final String msg = String.format("%-20s(%2d) | %10s | %s.%s", name, id, string, tr.getTestClass().getName(), tr.getName());
        System.out.println(msg);
        log.info(msg);
//        if (++m_count % 40 == 0) {
//            System.out.println("");
//        }
    }

}
