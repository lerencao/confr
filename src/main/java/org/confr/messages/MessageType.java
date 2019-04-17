package org.confr.messages;

public enum MessageType {
    WatchKeysRequest,
    WatchKeysResponse,
    MetadataRequest,
    MetadataResponse,
    ACK,
    PING,
    PONG, // sent from client to server
    ConfChangeResponse
}
