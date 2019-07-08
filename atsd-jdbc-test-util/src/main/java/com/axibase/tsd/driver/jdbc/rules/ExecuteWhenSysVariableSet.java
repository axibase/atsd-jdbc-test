package com.axibase.tsd.driver.jdbc.rules;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@Repeatable(RequiredVariables.class)
public @interface ExecuteWhenSysVariableSet {
    String value();
}
