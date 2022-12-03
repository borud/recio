# RecordIO

This library implements a simple `Reader` and `Writer` pair for length prefixed records. This is suitable for writing serialized data records to a disk file. The datastream consists of unsigned 32 bit integer (little endian) indicating the length of a payload, followed by the payload itself.

*It is more traditional to choose [network byte order](https://en.wikipedia.org/wiki/Endianness#Networking), which is big endian, but most systems this code will touch is going to be little-endian so that's what we use.*

When using this you have to make sure to give record sizes some thought.  When you read records you want the supplied buffer to be large enough to hold the messages you are reading.  If your target buffer isn't large enough you will get an `ErrTargetBufferTooSmall` error.
