
from hashlib import sha256
from time import time
try:
    from ubinascii import a2b_base64 as b64decode, b2a_base64 as b64encode
except ImportError:
    from base64 import b64encode, b64decode

from umqtt.simple import MQTTClient, MQTTException
from urllib.parse import quote_plus, urlencode


def _default_call_back(topic, msg):
    print("Topic: \"{topic}\", Message: \"{message}\"".format(topic=topic, message=msg))


class AzureMQTT:
    def __init__(self, connection_string: str, policy_name=None, expiry: int = 36000):
        self.params = dict(field.split('=', 1) for field in connection_string.split(';'))
        required_keys = ["HostName", "DeviceId", "SharedAccessKey"]
        if any(k not in self.params for k in required_keys):
            raise ValueError("connection_string is invalid, should be in the following format:",
                             "HostName=foo.bar;DeviceId=Fo0B4r;SharedAccessKey=Base64FooBar")
        self.sas_token = generate_sas_token(self.params["HostName"], self.params["SharedAccessKey"],
                                            policy_name=policy_name, expiry=expiry)
        self.username = "{host_name}/{device_id}/?api-version=2018-06-30".format(host_name=self.params["HostName"],
                                                                                 device_id=self.params["DeviceId"])
        self.password = self.sas_token

        self.mqtt_client = MQTTClient(client_id=self.params["DeviceId"], server=self.params["HostName"],
                                      user=self.username, password=self.password, ssl=True)
        self._default_subscribe_string = "devices/{device_id}/messages/devicebound/#".format(device_id=self.params["DeviceId"])

    def _default_subscribe(self):
        subscription_string = "devices/{device_id}/messages/devicebound/#".format(device_id=self.params["DeviceId"])
        self.mqtt_client.subscribe(subscription_string)

    def setup(self, callback=_default_call_back, subscribe_string: str = "default"):
        """
        An easy way to connect, set the callback, and subscribe to messages.
        :return:
        """
        self._connect()
        self.mqtt_client.set_callback(callback)
        if subscribe_string == "default":
            self.mqtt_client.subscribe(self._default_subscribe_string)
        else:
            self.mqtt_client.subscribe(subscribe_string)

    def _connect(self):
        """
        A relay to self.mqtt_client.connect(), but with errors that return usable info that doesn't require the spec
        sheet to figure out what's going on.
        :return:
        """
        try:
            self.mqtt_client.connect()
        except MQTTException as e:
            error_num = int(e.args[0])
            if error_num == 1:
                raise MQTTException("1: Server does not support level of MQTT protocol requested by the client.")
            elif error_num == 2:
                raise MQTTException("2: The Client identifier is correct UTF-8 but not allowed by the Server.")
            elif error_num == 3:
                raise MQTTException("3: The Network Connection has been made but the MQTT service is unavailable.")
            elif error_num == 4:
                raise MQTTException("4: The data in the user name or password is malformed.")
            elif error_num == 5:
                raise MQTTException("5: The client is not authorized to connect.")
            elif error_num >= 6:
                raise MQTTException(str(error_num) + ":",
                                    "The server reported an error not specified in the MQTT spec as of v3.1.1")

    def send(self, property_name: str, property_value: str):
        topic_string = "devices/{device_id}/messages/events/".format(device_id=self.params["DeviceId"])
        payload_string = "{%s=%s}" % (property_name, property_value)
        self.mqtt_client.publish(topic=topic_string, msg=payload_string)

    def wait_msg(self):
        self.mqtt_client.wait_msg()

    def check_msg(self):
        self.mqtt_client.check_msg()

    def print(self):
        print("Host Name:        ", self.params["HostName"])
        print("Device ID:        ", self.params["DeviceId"])
        print("Shared Access Key:", self.params["SharedAccessKey"])
        print("SAS Token:        ", self.sas_token)
        print("Username:         ", self.username)
        print("Password:         ", self.password)


def generate_sas_token(uri: str, key: str, policy_name=None, expiry: int = 36000) -> str:
    """
    Create an Azure SAS token.
    :param uri: URI/URL/Host Name to connect to with the token.
    :param key: The key.
    :param policy_name: Not sure what it is right now, defaults to None.
    :param expiry: How long until the token expires. defaults to one hour.
    :return: An SAS token to be used with Azure.
    """
    ttl = time() + expiry + 946684800
    sign_key = "{uri}\n{ttl}".format(uri=quote_plus(uri), ttl=int(ttl))
    signature = b64encode(hmac_digest(b64decode(key), sign_key.encode())).rstrip(b'\n')

    rawtoken = {
        'sr':  uri,
        'sig': signature,
        'se': str(int(ttl))
    }

    if policy_name is not None:
        rawtoken['skn'] = policy_name

    return 'SharedAccessSignature ' + urlencode(rawtoken)


def hmac_digest(key: bytes, message: bytes) -> bytes:
    """
    A MicroPython implementation of HMAC.digest(), because HMAC isn't accessible yet.
    :param key: key for the keyed hash object.
    :param message: input for the digest.
    :return: digest of the message passed in.
    """
    trans_5C = bytes((x ^ 0x5C) for x in range(256))
    trans_36 = bytes((x ^ 0x36) for x in range(256))
    inner = sha256()
    outer = sha256()
    blocksize = 64
    if len(key) > blocksize:
        key = sha256(key).digest()
    key = key + b'\x00' * (blocksize - len(key))
    inner.update(bytes_translate(key, trans_36))
    outer.update(bytes_translate(key, trans_5C))
    inner.update(message)
    outer.update(inner.digest())
    return outer.digest()


def bytes_translate(input_bytes: bytes, input_table: bytes):
    """
    A MicroPython implementation of bytes.translate, because that doesn't actually exist at minimum on the XBee.
    Essentially bytes.translate without using bytes.translate.
    :param input_bytes: Bytes to be run through the table.
    :param input_table: 256 byte table.
    :return: Input_Bytes, but run through the table.
    """
    if len(input_table) != 256:
        raise ValueError("Input table must be 256 bytes long.")
    output_bytes = []
    for byte in input_bytes:
        output_bytes.append(input_table[int(byte)])
    return bytes(output_bytes)
