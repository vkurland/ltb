package ltb;

import org.testng.ISuite;
import org.testng.ISuiteListener;

/**
 * Copyright (c) 2018 Happy Gears
 * author: vadim2
 * Date: 7/30/18
 */
public class TestSuiteListener implements ISuiteListener {

    @Override
    public void onStart(ISuite suite) {
        System.out.println("------- Test suite starts -------");
        TestUtils.startRedisServer();
    }

    @Override
    public void onFinish(ISuite suite) {
        System.out.println("------- Test suite ends   -------");
        TestUtils.stopRedisServer();
    }
}
