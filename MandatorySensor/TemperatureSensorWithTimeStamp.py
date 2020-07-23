import sys
import time
import datetime
import random


def main(streaming_directory: str, sensor_identifier1: str, sensor_identifier2: str, sensor_identifier3: str, sensor_identifier4: str) -> None:

    seconds_to_sleep = 5;

    while True:
        # Get current time
        current_time = str(time.time())

        # Create a new file name with the time stamp1
        file_name = streaming_directory + "/" + current_time + "sensor1" + ".csv"

        # Get the temperature
        temperature_value = random.randrange(5.0, 19.0)

        # Write the current temperature value in the file

        with (open(file_name, "w")) as file:
            time_stamp = datetime.datetime.now()
            file.write(sensor_identifier1 + "," + str(temperature_value) + "," + str(time_stamp))
        print("Wrote file with temperature for sensor1" + str(temperature_value))

        # Create a new file name with the time stamp2
        file_name = streaming_directory + "/" + current_time + "sensor2" + ".csv"

        # Get the temperature
        temperature_value = random.randrange(9.0, 19.0)

        # Write the current temperature value in the file

        with (open(file_name, "w")) as file:
            time_stamp = datetime.datetime.now()
            file.write(sensor_identifier2 + "," + str(temperature_value) + "," + str(time_stamp))
        print("Wrote file with temperature for sensor2" + str(temperature_value))

        # Create a new file name with the time stamp3
        file_name = streaming_directory + "/" + current_time + "sensor3" + ".csv"

        # Get the temperature
        temperature_value = random.randrange(10.0, 29.0)

        # Write the current temperature value in the file

        with (open(file_name, "w")) as file:
            time_stamp = datetime.datetime.now()
            file.write(sensor_identifier3 + "," + str(temperature_value) + "," + str(time_stamp))
        print("Wrote file with temperature for sensor3" + str(temperature_value))

        # Create a new file name with the time stamp4
        file_name = streaming_directory + "/" + current_time + "sensor4" + ".csv"

        # Get the temperature
        temperature_value = random.randrange(20.0, 50.0)

        # Write the current temperature value in the file

        with (open(file_name, "w")) as file:
            time_stamp = datetime.datetime.now()
            file.write(sensor_identifier4 + "," + str(temperature_value) + "," + str(time_stamp))
        print("Wrote file with temperature for sensor4" + str(temperature_value))

        # Sleep for a few seconds
        time.sleep(seconds_to_sleep)


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Usage: python TemperatureSensorWithTimeStamp directory sensorIdentifier", file=sys.stderr)
        exit(-1)

    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],sys.argv[5])
