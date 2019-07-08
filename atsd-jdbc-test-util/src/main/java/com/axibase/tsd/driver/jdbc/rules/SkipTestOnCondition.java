package com.axibase.tsd.driver.jdbc.rules;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.Annotation;

public class SkipTestOnCondition implements TestRule {
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                for (Annotation annotation : description.getAnnotations()) {
                    if (annotation.annotationType().equals(ExecuteWhenSysVariableSet.class)) {
                        String variable = ((ExecuteWhenSysVariableSet) annotation).value();
                        assumeTableIsSet(variable);
                    }
                }
                base.evaluate();
            }

            private void assumeTableIsSet(String variable) {
                if (StringUtils.isEmpty(System.getProperty(variable))) {
                    Assume.assumeTrue("Required variable is not set: \"" + variable + "\"", false);
                }
            }
        };
    }
}
