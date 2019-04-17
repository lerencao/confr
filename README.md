### Confr ###

- [X] Sync configuration from server to client.
- [X] Client send watch request to server to watch config keys.
- [X] Any change to configuration will be pushed from server to client.
- [X] Client reconnects to server(or other server) when disconnected, and re-watch the same keys.
- [ ] Versioned Config.
- [ ] Client send sync request to server if no data change happens for some duration, say 5s.
- [ ] server configuration.


#### TODO ####

- Add tests.
- Implement versioned config change.
- Channel close pipline revisited.
- Many more...