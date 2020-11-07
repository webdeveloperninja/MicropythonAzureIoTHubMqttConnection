import network
from azure_mqtt_connection import AzureMQTT

from time import sleep

def main():
    IoTHubConnectionString = "-- pull from configuration --"

    conn = network.Cellular()
    while not conn.isconnected():
        print("Waiting for network connection...")
        sleep(4)
    print("Network connected")

    device = AzureMQTT(IoTHubConnectionString)
    print("Connecting to Azure...")
    device.setup()
    print("Azure connected")
    device.print()

    for i in range(0, 10):
        device.send('send', str(i))
        sleep(1)

    # Loop forever checking for messages from Azure
    print("Checking for messages for Azure...")
    while True:
        message = device.check_msg()
        if message:
            print(message)
        sleep(1)


main()