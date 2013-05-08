package com.xing.beetle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class RedisFailoverManager implements Runnable {

    private static Logger log = LoggerFactory.getLogger(RedisFailoverManager.class);

    private String currentMaster;
    private final String masterFile;

    public RedisFailoverManager(String masterFile, RedisConfiguration initialConfig) {
        this.masterFile = masterFile;
        this.currentMaster = initialConfig.getHostname() + ":" + initialConfig.getPort();
    }
    @Override
    public void run() {
        log.info("Redis failover manager running.");

        while (true) {
            try {
                String masterInFile = readCurrentMaster();
                if (!currentMaster.equals(masterInFile)) {
                    log.warn("Redis master switch! " + currentMaster + " -> " + masterInFile);

                    // Re-connect.

                    currentMaster = masterInFile;
                }
            } catch(Exception e) {
                log.error("Error when trying to read current Redis master. Retrying.", e);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.info("Interrupted.");
                return;
            }
        }
    }

    private String readCurrentMaster() throws IOException {
        FileInputStream stream = new FileInputStream(new File(masterFile));

        try {
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());

            return sanitizeMasterString(Charset.forName("UTF-8").decode(bb).toString());
        } finally {
            stream.close();
        }
    }

    private String sanitizeMasterString(String masterString) {
        if (masterString != null) {
            return masterString.replace("\n", "").replace("\r", "");
        }

        return masterString;
    }

    public String getCurrentMaster() {
        return currentMaster;
    }

    public String getMasterFile() {
        return masterFile;
    }

    public boolean hasMasterFile() {
        return masterFile != null && !masterFile.isEmpty();
    }

}
