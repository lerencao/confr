package org.confr.config;

public class ConfServerConfig {
    private final String bindAddress;
    private final int bindPort;

    public ConfServerConfig(String bindAddress, int bindPort) {
        this.bindAddress = bindAddress;
        this.bindPort = bindPort;
    }

    public int getBindPort() {
        return bindPort;
    }

    public String getBindAddress() {
        return bindAddress;
    }
}
