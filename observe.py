# basic govee bluetooth data reading code for model https://amzn.to/3z14BIi
# written/modified by Austin of austinsnerdythings.com 2021-12-27
# original source: https://gist.github.com/tchen/65d6b29a20dd1ef01b210538143c0bf4
import logging
import json
from time import sleep

from basic_mqtt import basic_mqtt
from bleson import get_provider, Observer

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.WARNING,
)

# I did write all the mqtt stuff. I kept it in a separate class
mqtt = basic_mqtt()
mqtt.connect()

# writing code on my windows computer, committing, pushing, and then
# pulling the new code on the Raspberry Pi is a tedious "debug" process.
# there are quite a few errors that spit out from bleson, which as far
# as I can tell, isn't super polished. if we set the debug level to
# critical, most of those disappear.
logging.getLogger("bleson").setLevel(logging.CRITICAL)

# basic celsius to fahrenheit function
def c2f(val):
    return round(32 + 9 * val / 5, 2)


last = {}

# I didn't write this, but it takes the raw BT data and spits out the data of interest
def temp_hum(values, battery, address):
    # the data has a 1 bit in the first position if it's negative.
    mult = 1
    if values[0] & 0x80 == 128:
        mult = -1
        values = bytes(values[0] & 0x7ff) + values[1:]
    values = int.from_bytes(values, "big")
    temp = float(values / 10000) * mult
    hum = float((values % 1000) / 10)
    # this print looks like this:
    # 2021-12-27T11:22:17.040469 BDAddress('A4:C1:38:9F:1B:A9') Temp: 45.91 F  Humidity: 25.8 %  Battery: 100 %
    logging.info(f"decoded values: {address} Temp: {c2f(temp)} F  Humidity: {hum} %  Battery: {battery}")
    # this code originally just printed the data, but we need it to publish to mqtt.
    # added the return values to be used elsewhere
    return c2f(temp), hum, battery


def on_advertisement(advertisement):
    # print(advertisement)
    mfg_data = advertisement.mfg_data

    # there are lots of BLE advertisements flying around. only look at ones that have mfg_data
    if mfg_data is not None:
        # print(advertisement)
        # there are a few Govee models and the data is in different positions depending on which
        # the unit of interest isn't either of these hardcoded values, so they are skipped
        if advertisement.name is not None:
            # this is where all of the advertisements for our unit of interest will be processed
            # print(advertisement)
            address = advertisement.address
            if "GVH" in advertisement.name:
                temp_f, hum, battery = temp_hum(mfg_data[3:6], mfg_data[6], address)

                if temp_f > 180.0 or temp_f < -30.0:
                    print(f"Invalid temperature unpacked: {temp_f}")
                    return
                # as far as I can tell bleson doesn't have a string representation of the MAC address
                # address is of type BDAddress(). str(address) is BDAddress('A4:C1:38:9F:1B:A9')
                # substring 11:-2 is just the MAC address
                mac_addr = str(address)[11:-2]

                # construct dict with relevant info
                msg = {"temp_f": temp_f, "hum": hum, "batt": battery, "rssi": advertisement.rssi}

                # looks like this:
                # msg data: {'temp_f': 45.73, 'hum': 25.5, 'batt': 100}
                logging.info(f"MQTT msg data: {msg}")

                # turn into JSON for publishing
                json_string = json.dumps(msg, indent=4)

                # publish to topic separated by MAC address
                mqtt.publish(f"govee/{mac_addr}", json_string)


# base stuff from the original gist
logging.warning(f"initializing bluetooth")
adapter = get_provider().get_adapter()
observer = Observer(adapter)
observer.on_advertising_data = on_advertisement

logging.warning(f"starting observer")
observer.start()
logging.warning(f"listening for events and publishing to MQTT")
try:
    while True:
        # unsure about this loop and how much of a delay works
        sleep(1)
except KeyboardInterrupt:
    observer.stop()
