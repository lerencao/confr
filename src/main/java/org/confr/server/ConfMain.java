package org.confr.server;

import org.confr.config.ConfServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfMain {
    private static final Logger logger = LoggerFactory.getLogger(ConfMain.class);
    public static void main(String[] args) {
        int exitCode = 0;
        try {
            // TODO: pass config to ConfServer
            ConfServer server = new ConfServer(new ConfServerConfig("0.0.0.0", 8844));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Received shutdown signal. Shutting down ConfServer");
                server.shutdown();
            }));

            server.start();
            server.awaitShutdown();
        } catch (Exception e) {
            logger.error("Exception during bootstrap of ConfServer", e);
            exitCode = 1;
        }

        logger.info("Exiting ConfServer");
        System.exit(exitCode);
    }

}
