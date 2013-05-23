package com.xing.beetle.examples;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public class AsyncStreamCopier {

    private final InputStream in;
    private final OutputStream out;
    private final String name;

    public AsyncStreamCopier(InputStream in, OutputStream out, String name) {
        this.in = in;
        this.out = out;
        this.name = name;
    }

    public void start() {
        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    int d;
                    while ((d = in.read()) != -1) {
                        out.write(d);
                    }
                } catch (IOException ignored) {
                }
            }
        }, name);

        t.setDaemon(true);
        t.start();
    }
}
