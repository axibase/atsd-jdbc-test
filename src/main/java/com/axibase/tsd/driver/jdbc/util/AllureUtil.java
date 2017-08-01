package com.axibase.tsd.driver.jdbc.util;

import com.google.common.base.Preconditions;
import io.qameta.allure.Attachment;

public class AllureUtil {
    @Attachment(value = "Log Output")
    public static <T> String log(String name, T logObject) {
        Preconditions.checkNotNull(logObject, "logObject is null");
        return logObject.toString();
    }
}
