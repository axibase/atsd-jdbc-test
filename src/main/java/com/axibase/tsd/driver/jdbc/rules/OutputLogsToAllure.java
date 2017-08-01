package com.axibase.tsd.driver.jdbc.rules;

import com.axibase.tsd.driver.jdbc.util.AllureUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

public class OutputLogsToAllure implements TestRule {
    private final boolean enable;
    private CaptureOutputStream captureOut;
    private CaptureOutputStream captureErr;
    private ByteArrayOutputStream copy;

    public OutputLogsToAllure(boolean enable) {
        this.enable = enable;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        if (enable) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    captureOutput();
                    try {
                        base.evaluate();
                    } finally {
                        flush();
                        final String capturedLog = copy.toString();
                        if (StringUtils.isNotEmpty(capturedLog)) {
                            AllureUtil.log(description.getDisplayName(), capturedLog);
                        }
                        releaseOutput();
                    }
                }
            };
        } else {
            return base;
        }
    }

    protected void captureOutput() {
        this.copy = new ByteArrayOutputStream();
        this.captureOut = new CaptureOutputStream(System.out, this.copy);
        this.captureErr = new CaptureOutputStream(System.err, this.copy);
        System.setOut(new PrintStream(this.captureOut));
        System.setErr(new PrintStream(this.captureErr));
    }

    protected void releaseOutput() {
        System.setOut(this.captureOut.getOriginal());
        System.setErr(this.captureErr.getOriginal());
        this.copy = null;
    }

    public void flush() {
        try {
            this.captureOut.flush();
            this.captureErr.flush();
        }
        catch (IOException ex) {
            // ignore
        }
    }

    private static class CaptureOutputStream extends OutputStream {

        private final PrintStream original;

        private final OutputStream copy;

        CaptureOutputStream(PrintStream original, OutputStream copy) {
            this.original = original;
            this.copy = copy;
        }

        @Override
        public void write(int b) throws IOException {
            this.copy.write(b);
            this.original.write(b);
            this.original.flush();
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            this.copy.write(b, off, len);
            this.original.write(b, off, len);
        }

        public PrintStream getOriginal() {
            return this.original;
        }

        @Override
        public void flush() throws IOException {
            this.copy.flush();
            this.original.flush();
        }

    }
}
