"""Collect the raw data from the sensor and store measurements in CouchDB"""
import logging
import datetime
import serial
import json
from restkit import Resource

DEFAULT_SERIAL_DEVICE = '/dev/ttyUSB0'
DEFAULT_BAUD_RATE = 9600


def collect_from_zigbee(target, stream_id, device=DEFAULT_SERIAL_DEVICE,
                        baud_rate=DEFAULT_BAUD_RATE):
    """Synchronously read from the serial port to target

    target can either be the URL of a couchdb server or a restkit resource
    instance.
    """
    from xbee import ZigBee

    if not hasattr(target, 'post'):
        target = Resource(target)

    serial_device = serial.Serial(device, baud_rate)
    headers = {'Content-Type': 'application/json'}
    if stream_id is None:
        stream_prefix = ""
    else:
        stream_prefix = stream_id + "_"
    try:
        xbee = ZigBee(serial_device, escaped=True)
        # Continuously read and print packets
        while True:
            try:
                response = xbee.wait_read_frame()
                now = datetime.datetime.utcnow()
                timestamp = now.replace(microsecond=0).isoformat() + 'Z'
                logging.debug(response)
                samples = response.get('samples', [])
                for sample in samples:
                    for sensor_id, sensor_value in sample.iteritems():
                        target.post(headers=headers, payload=json.dumps({
                            # TODO: use a mapping mechanism instead of stream_id
                            # prefix
                            'datastream_id': stream_prefix + sensor_id,
                            'value': sensor_value,
                            'timestamp': timestamp,
                        }))

            except KeyboardInterrupt:
                break
    finally:
        serial_device.close()


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-t", "--device-type", dest="device_type",
                      default="xbee-zb",
                      help="Type of device (to read from the serial port)")
    parser.add_option("-d", "--device", dest="device",
                      default='/dev/ttyUSB0',
                      help="Serial device to read the data from.")
    parser.add_option("-b", "--baud-rate", dest="baud_rate",
                      default=9600,
                      help="Baud rate of the serial device.")
    parser.add_option("-i", "--stream-id", dest="stream_id",
                      help="ID of the stream to use in the database entries.")
    parser.add_option("-c", "--couchdb-url", dest="couchdb_url",
                      default="http://localhost:5984/instrumentalist",
                      help="URL of the target CouchDB database.")
    (options, args) = parser.parse_args()

    resource = Resource(options.couchdb_url)
    if options.device_type == "xbee-zb":
        print "Collecting data from %s to store in %s" % (
            options.device, options.couchdb_url)
        collect_from_zigbee(resource, options.stream_id, options.device,
                            options.baud_rate)
    else:
        print "Unsupported device type: " + options.device_type

