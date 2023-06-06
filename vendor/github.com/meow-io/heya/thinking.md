# Meow relay

A server for group-based mailboxes.
Able to register iPhone pushes to devices that are interested.

Mutual TLS authentication
Group mailbox is identified by the certificate
Messages are given a monotonic sequence number
The protocol itself for fetching messages is fetching one at a time or a range
Could be simply the sparse range thing
Request is

looks like something called "silent" notifications are needed too, this gives the app a chance to wake up and get the new events and process them

this is the api:
register device token with group mailbox
deregister device token with group mailbox
get x messages
post a new message

really, lets just make this a normal HTTPS api?
or ... websocket?

websocket would actually probably be a fine fit

so, mutual TLS, one connection per mailbox?
is there a way to share a single connection per host?

if the autnetication mechanism is totally separate, and then the relay server uses...

catbox
or .. webpub?

request\n{id}
	token\n{id}\n{token}\n{capabilities?}

register\n{token}
	registered\n{token}

deregister\n{token}
	degistered\n{token}

post\n{len as string}\n{digest}\n{message}\n
get\n{base as string}\n{offset}\n ... \n\n

...

account creation?
authenticate

it would in many ways be a little bit less messy if this worked more like rabbit

you authenticate via MTLS
you add tokens to your connection

the benefit would be no private keys leave the device
you couldn't track who was connecting in the group

the private key isn't THAT important tho, not in this specific case
because the message itself is encrypted at rest through some other means

in fact, you could argue you only need the active connection for which roost your on open at a time, the rest you're getting push notifications for the rest

so, is it per user or per group?

sharing credentials obfuscates who is accessing what
and the credentials aren't important to the secrecy of the message
the shared header private key is, but even that doesn't reveal the message contents, just some of the metadata

nothing about this precludes individual accounts either

so, can one mailbox deliver mail to another mailbox?
if we disconnect sending from receiving, then its no problem

ok ...

so, lets disconnect it

you log in with an ephemeral key and don't prove anything at all about the client
you can then register to send or recv on a per pubkey basis
and you can register a key to get email at

this would be a little more elegant in terms of connections
it would make fingerprinting easy. but lets ignore that issue for now

so, then, what are the flows we need for this?

* send a message to a key
* recv messages for an id (is this a pub key?)
* register a push token for a mailbox
* deregister a push token
* register a new pubkey

this paper is still very relevant
https://cs-people.bu.edu/kaptchuk/publications/ndss21.pdf
and maybe this?
https://eprint.iacr.org/2021/1380.pdf

we need to decide what we want to balance here?
the improvement of anonymous mailboxes sounds about right
with clear times on them at time of initialization
if we allow delivery of a message to multiple mailboxes, that is a nice improvement
but it also completely destroys the anonmynity
this really suggests i should have stuck to pairwise delivery at the very beginning

this would massive simplify the protocol

otoh, this really moves far away from a shared group mailbox for delivery
but whats happening here is im not being careful about perscribing where we need
traffic mixing (or delivery mixing) to obfuscate details about the sender and group
membership

the group delivery stuff is also the tension between bandwidth and security

i think group delivery is still the way to go

what are the questions?

group vs individual accounts?
mititage against SDA?
meow specific vs general?

if we take the group, meow-specific, don't care about SDAs route (which is a good route)

we end up with group mailboxes with the same mTLS, public key is the account name
reading and writing is all the same

i want the simplist design, so this has a lot of appeal
in this world there would also be a TTL?
there would definitely be a TTL on messages themselves.

so, this protocol would be pretty simple then.

mTLA handles which mailbox you're in, and any authentication concerns

fn:{host}:{type}:{priv-der in base64url}

fn:raq.friendnet.io:

https://www.rfc-editor.org/rfc/rfc5915
https://github.com/golang/go/blob/master/src/crypto/x509/sec1.go

i need to actually code this up to get a sense of what a url would look like
i'd like to omit the public key for the sake of space

fn:abc.friendnet.io:rfc5915:{base64 bytes}

something like that?

the actual wire format

// sent async
MSG {seqnum} {bytes-len}\n
{length bytes follow}\n

SEND {digest-hex} {len-ascii}\n
{length bytes follow}\n
RECV {digest-hex} {seq-ascii}\n

REQ {seq-ascii}\n

IOSADD {device token}\n
IOSADD {device token}\n

IOSDEL {device token}\n
IOSDEL {device token}\n

this would be the whole thing! very f'n simple

i say .. lets code up this very simple version and try it out

wait, there is one gap in this approach, namely, account creation

when you log in with an unrecognized cert your first request MUST be

REG {token}\n
REG {token}\n
or close connection
...

... okay, lets try this again, new command structure to follow, a little more human readable

client
	server

SEND {digest-hex} {len-ascii}\n{body}\n
	RECV {digest-hex} {seq-ascii}\n
WANT {seq-ascii}\n
	SEND {seq-ascii} {len-ascii}\n
	{body}\n
IOSA {device-token-ascii}\n
	IOSA {device-token-ascii}\n
IOSD {device-token-ascii}\n
	IOSD {device-token-ascii}\n
SEYA\n
	(quit and revoke the currently used certificate)
QUIT\n

server can send at any time
	HAVE {seq-ascii} {len-ascii}\n

on initial start

HEYA {token}\n

responses are not guaranteed to be in order

https://pkg.go.dev/github.com/sideshow/apns2

ok ... so,

there are a bunch of lingering concerns

When you connect to a server you want to get the current message index
as well, you want to get notifications for any messages after that


so, from an order perspective, you add a channel if it doesn't exist and then you get the latest message

okay, ideally it would be something like this

[ ] when you create a connection, you create a special struct for each key joined
this is responsible for ..
sending HAVE commands down to all the connections interested
managing all the connections per key
it would poll the db, maintain the last key seen and listen to rabbitmq

[ ] when any message is persisted, it has to look up all the relevant device tokens and do the right thing

so .. here is the other big concern from a protocol perspective, seeing as i can't send a background notification
i need to know if you're connected and you don't want a push notification, that is, suppress it if its from yourself

ok ... i need to redo everything

TLS

FROM {key-sha}
STOP {key-sha}

everything is point to point ... we don't try to get super efficent with bandwidth

ok ... so, you can register a token for recv messages

okay operation p2p goes like this

[ ] redo HEYA
	[ ] send token based

[ ] eliminate multikey messages
	[ ] make same priority messages pick randomly
	[ ] the "send list" is now the desc + changes
[ ] envelope protection is per-protocol
