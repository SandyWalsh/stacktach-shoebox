import calendar
import datetime
import json
import struct

def handle_datetime(obj):
    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
    millis = int(
        calendar.timegm(obj.timetuple()) * 1000 +
        obj.microsecond / 1000
    )
    return millis


def InvalidVersion(Exception):
    pass

def OutOfSync(Exception):
    pass

BOR_MAGIC_NUMBER = 0x69867884

class Version1(object):
    # Version 1 SCHEMA
    # ----------------
    # i = 0x69867884  (EVNT)
    # h = version
    # i = metadata block length
    # i = raw notification block length
    # i = 0x00000000 EOR

    # Metadata dict block
    # i = number of strings (N) - key/value = 2 strings
    # N * i = length of key followed by length of value
    # N * (*s) = key followed by value

    # Raw notification block
    # i = length of raw data block
    # *s = raw data

    # EXAMPLE
    #--------
    # With above Event and Metadata
    #
    # Header schema: "ihiii"
    # Metadata length: 119
    # Raw notification length: 201
    # Metadata = 6 strings (3 key-value pairs)
    # Metadata schema: "iiiiiii6s14s10s31s10s20s"
    #                                     ------ key/value
    #                               ------ key/value
    #                          ----- key/value
    #                    ------ length of the 6 strings
    #                   - 12 entries (6 string sizes + 6 strings)
    # Raw notification: "i197s"
    #                     ---- json notification
    #                    - 197

    def __init__(self):
        self.header_schema = "ihiii"
        self.header_size = struct.calcsize(self.header_schema)

    def pack(self, notification, metadata):
        nsize = len(notification)
        raw_block_schema = "i%ds" % nsize
        raw_block = struct.pack(raw_block_schema, nsize, notification)

        metadata_items = ["i"] # appended with N "%ds"'s
        metadata_values = [len(metadata) * 4]  # [n]=key, [n+1]=value
        for key, value in metadata.iteritems():
            metadata_items.append("i")
            metadata_items.append("i")
            metadata_values.append(len(key))
            metadata_values.append(len(value))

        for key, value in metadata.iteritems():
            metadata_items.append("%ds" % len(key))
            metadata_values.append(key)
            metadata_items.append("%ds" % len(value))
            metadata_values.append(value)
        metadata_schema = "".join(metadata_items)

        metadata = struct.pack(metadata_schema, *metadata_values)

        header = struct.pack(self.header_schema, BOR_MAGIC_NUMBER, 1,
                             struct.calcsize(metadata_schema),
                             struct.calcsize(raw_block_schema), 0)

        return (header, metadata, raw_block)

    def unpack(self, file_handle):
        header_bytes = file_handle.read(self.header_size)
        header = struct.unpack(self.header_schema, header_bytes)

        if header[0] != BOR_MAGIC_NUMBER:
            raise OutOfSync()
        if header[1] != 1:
            raise InvalidVersion("Expected V1, got V%d" % header[1])

        metadata_bytes = file_handle.read(header[2])
        num_strings = struct.unpack_from("i", metadata_bytes)
        offset = struct.calcsize("i")
        lengths = num_strings[0] / 2
        lengths_schema = "i" * lengths
        key_value_sizes = struct.unpack_from(lengths_schema, metadata_bytes,
                                             offset=offset)
        key_value_schema_list = ["%ds" % sz for sz in key_value_sizes]
        key_value_schema = "".join(key_value_schema_list)
        offset += struct.calcsize(lengths_schema)
        key_values = struct.unpack_from(key_value_schema, metadata_bytes,
                                        offset=offset)
        metadata = dict((key_values[n], key_values[n+1])
                        for n in range(len(key_values))[::2])

        raw = file_handle.read(header[3])
        raw_len = struct.unpack_from("i", raw)
        offset = struct.calcsize("i")
        raw_json = struct.unpack_from("%ds" % raw_len[0], raw, offset=offset)
        notification = json.loads(raw_json[0])

        return (metadata, notification)


VERSIONS = {1: Version1()}
CURRENT_VERSION = 1

def get_version_handler(version=CURRENT_VERSION):
    global VERSIONS

    version_handler = VERSIONS.get(version)
    if not version_handler:
        raise InvalidVersion()
    return version_handler


def pack_notification(notification, metadata, version=CURRENT_VERSION):
    version_handler = get_version_handler(version)
    return version_handler.pack(notification, metadata)


def unpack_notification(file_handle, version=CURRENT_VERSION):
    version_handler = get_version_handler(version)
    return version_handler.unpack(file_handle)

event = {"event_type": "nova.compute.run_instance.start",
         "generated": datetime.datetime.utcnow(),
         "request_id": "req-1234abcd5678efgh",
         "source": "n-compute-1973",
         "payload": {
            "foo": 123,
            "blah": "abc",
            "zoo": False
         }
        }

json_event = json.dumps(event, default=handle_datetime)
metadata = {'request_id': event['request_id'],
            'event_type': event['event_type'],
            'source': event['source'],
           }


binary = pack_notification(json_event, metadata)

with open("test.dat", "wb") as f:
    for block in binary:
        f.write(block)

with open("test.dat", "rb") as f:
    metadata, notification = unpack_notification(f)
    print "Metadata:", metadata
    print "Notification:", notification
