// This package defines (yet another) bencode encoding/decoding library. What is special about this
// approach is it uses tags for mapping struct fields to bencode properties. As well, it has support for fixed-byte array
// map keys.
//
// The serialization/deseriazation functions expect to be annotated with `bencode:".."` tags in the structs they serialize/deserialize to.
package bencode

const (
	numberStart    = 0x69
	dictStart      = 0x64
	listStart      = 0x6c
	bencodeEnd     = 0x65
	bytesLengthSep = 0x3a
)
