# Heya

A communication protocol & server for sending and receiving messages.

## Goals

* Support binary data
* Transport encryption
* Sender anonymity
* Application agnostic

### Anti-goals

* Server-side indexing
* At-rest encryption for message data

## Concepts

Client operate in one of two modes, as an anonymous sender or as a receiver. To receive messages a client sends a token which designated the TLS certificate used in the connection as capable of receiving messages. Then any number of sending tokens are issued which can be distributed to parties who wish to send messages to this client.

Messages are assigned monotonic contiguous sequence numbers starting with 0 and are arbitrary byte sequences. Sequence numbers are associated with the sending token they were sent with.

## Protocol

All commands are 4-letter followed by some number of arguments. Arguments are separated by a single space and server and client commands are terminated by a `\n`. Server responses occur in the order in which client requests are received. The server can also send the `HAVE` command at any time.

The default port for the server is 8337.

### Initial connection

The server is connected to over TCP using TLS. Once a connection is established the server begins by sending `HEYA [version-number:ascii]\n`. If the client certificate is capable of receiving messages and has send tokens associated with it, then the server will send one message per sending token indicating the sequence number of the last message sent to that sending token. The format for that message is `LAST [token-hex:64] [seq:ascii number]`. seq + 1.  Finally, in all cases, the server respond with `DONE` indicating it is ready to receive commands. Commands sent before `DONE` is received are ignored.

### Registering as an inbox

To allow messages to be received, the client needs to send `INCO [access-token]`. This will then let any future connections made with the same client TLS certificate to receive messages.

```
client: INCO [inbox-token-hex:64] --->
server: <--- INCO [inbox-token:64]\n
```

If the inbox token isn't recognized or there is an error the server will immediately close the connection.

### Obtaining a sending token

To allow other clients to send messages to your inbox you must first obtain a send token from the server.

```
client: AUTH [start-time:0-10 ascii] [end-time:0-10 ascii] --->
server: <--- AUTH [token-hex:64] [start-time] [end-time]
```

### Revoking a sending token

To allow other clients to send messages to your inbox you must first obtain a send token from the server.

```
client: DEAU [token-hex:64] --->
server: <--- DEAU [token-hex:64]
```

### Extending a sending token

To allow other clients to send messages to your inbox you must first obtain a send token from the server.

```
client: EXTD [token-hex:64] [seconds:ascii-int] --->
server: <--- EXTD [token-key] [new-end-time:ascii-int]
```

### Listing sending tokens

List all the send tokens available for this key.

```
client: LIST\n--->
server: <--- LIST [len:ascii-int]\n[token] [seq:ascii-int] [start-time:ascii-int] [end-time:ascii-int]\n
```

### Sending a message

Clients send a message by using `SEND [send-token] [digest-hex] [feature-flags] [length]\n[len bytes]` where `send-token` is the sending token, `digest-hex` is the sha256 digest of the body to be send and `len` is ascii decimal encoded length of the body. Servers respond with `RECV [digest-hex]` where `digest` matches the digest sent.

```
client: SEND [send-token-hex:64] [sha256-digest-hex] [body-length]\n[len bytes] --->
server: <--- RECV [digest-hex]\n
```

If an error is encountered while executing this command the server will close the connection.

### Receiving a message

The server initiates by sending `HAVE [seq]` where `seq` is the ascii encoded sequence number of the message. The client requests the message by sending `WANT [seq]` where `seq` is the ascii-encoded sequence number of the message being requested. The server responds with `GIVE [token] [seq] [length]\n[len bytes]` where `seq` is the ascii-encoded sequence number of the message being sent and `len` os the ascii-encoded length of the body.

```
server: <--- HAVE [token] [seq]
client: WANT [token] [seq]\n --->
server: <--- GIVE [token] [seq] [length]\n[len bytes]
```

If a client requests a message that doesn't exist it will send back `GONE (seq)`.

```
client: WANT [token] [seq]\n --->
server: <--- GONE [token] [seq]\n
```

### Trim messages

```
client: TRIM [token] [seq]\n --->
server: <--- TRIM [token] [seq] [num]\n
```

### iOS Push notifications

Client can register a device push token for their mailbox using `IOSA {device-token:ascii}`. The push token can be up to 64 characters long. The server acknowledges with `IOSA {device-token}`.

```
client: IOSA [token]\n --->
server: <--- IOSA [token]\n
```

The client can also deregister their push token using `IOSD {device-token:ascii}`. The server acknowledges with `IOSD {device-token:ascii}`.

If an error is encountered during the send it will close the connection. An attempt to add an token to a non-mailbox will close the connection.


```
client: IOSD [token]\n --->
server: <--- IOSD [token]\n
```

If an error is encountered during the send it will close the connection.

Clients can list their currently used push notification tokens by issuing `IOSL`.

```
client: IOSL\n--->
server: <--- IOSL [len:ascii-int]\n[token] [start-time]\n{repeated}
```

### Pinging

Clients can send `PING` messages. They will recieve a `PONG` back for every ping sent.

```
client: PING\n --->
server: <--- PONG\n
```

### Deregistering an inbox

A client can deregister an inbox by sending `DALL` which stands for deauth all. All messages, send tokens and push tokens associated with that inbox are immediately destroyed.

```
client: DALL\n --->
server: <--- DALL\n
```

### Closing

To close the connection the client sends `QUIT`. This closes the conenction and no further communication is sent to the client.

## Running

soemthing something about running the server. path to APN certification here

## Alternatives considered

### Email

* ascii-centric, not binary, so, everything is base64 encoded
* very application-centric
* no ability to send ios push notifications
* exposes sender metadata

### Signal server

* tied to phone numbers for device identification

### HTTP-based server

* this is how signal works
* desire to limit surface area of protocol led to a precautionary limiting of protocols this is built on

