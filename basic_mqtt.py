# basic MQTT helper class. really needed to write one of these to simplify basic MQTT operations
# written/modified by Austin of austinsnerdythings.com 2021-12-27
# original source: https://gist.github.com/fisherds/f302b253cf7a11c2a0d814acd424b9bb
# filename is basic_mqtt.py
from paho.mqtt import client as mqtt_client
import logging
import datetime

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


mqtt_host = "mqtt.bluesmoke.network"
test_topic = "mqtt_test_topic"

# this is really not polished. it was a stream of consciousness project to pound
# something out to do basic MQTT publish stuff in a reusable fashion.
class basic_mqtt:
    def __init__(self):
        self.client = mqtt_client.Client()
        self.subscription_topic_name = None
        self.publish_topic_name = None
        self.callback = None
        self.host = mqtt_host

    def connect(self):
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        logging.info(f"connecting to MQTT broker at {mqtt_host}")
        self.client.connect(host=mqtt_host, keepalive=30)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        # self.client.subscribe("$SYS/#")

    def on_message(self, client, userdata, msg):
        print(f"got message of topic {msg.topic} with payload {msg.payload}")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def publish(self, topic, msg):
        self.client.publish(topic=topic, payload=msg)

    def subscribe_to_test_topic(self, topic=test_topic):
        self.client.subscribe(topic)

    def send_test_message(self, topic=test_topic):

        self.publish(topic=topic, msg=f"test message from python script at {datetime.datetime.now()}")

    def disconnect(self):
        self.client.disconnect()

    def loop(self):
        self.client.loop_forever()


if __name__ == "__main__":
    logging.info("running MQTT test")
    mqtt_helper = basic_mqtt()
    mqtt_helper.connect()
    mqtt_helper.subscribe_to_test_topic()
    mqtt_helper.send_test_message()
    mqtt_helper.disconnect()
