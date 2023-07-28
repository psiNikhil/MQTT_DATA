import pandas as pd
import paho.mqtt.client as mqtt

class dataBases :
    class AutoDiscoveries:
            # ... (class definition as before)
            def __init__(self, **kwargs):
            # Initialize the attributes based on the given keyword arguments
                for key, value in kwargs.items():
                    setattr(self, key, value)

            
                    
        

            @classmethod
            def from_mqtt_message(cls, mqtt_message):
                # Parse the MQTT message and extract the necessary data to create the instance
                # Modify this method to match the format of the MQTT message you receive
                data = mqtt_message.payload.decode()
                data_parts = data.split(',')  # Assuming the data is comma-separated
                try:
                    data_dict = eval(data)
                # Check if the received data contains the same number of attributes as AutoDiscoveries
                    if len(data_parts) == len(cls.__init__.__code__.co_varnames) - 1:  # Subtract 1 for 'self'
                    # The received data matches the AutoDiscoveries class attributes
                        return cls(*data_parts)
                    else:
                    # Create a generic instance to hold the data as key-value pairs
                        return cls(data_dict={f'col{i}': val for i, val in enumerate(data_parts)})
                
                except SyntaxError:
                # If the data is not in dictionary format, create a single cell instance
                         return cls(value=data)

            
            
            
            def add_extra_columns_to_df(self, df):
                # This method adds any extra columns present in the instance to the DataFrame.
                # The DataFrame 'df' is modified in place.
                extra_columns = self.__dict__.copy()
                for attribute in self.__dict__:
                    # Remove the predefined attributes to keep only the extra columns
                    if hasattr(self, attribute):
                        extra_columns.pop(attribute)

                for column_name, column_value in extra_columns.items():
                    df[column_name] = [column_value]

            def update_data(self, data):
            # Update the data based on the received data
                if isinstance(data, dict):
                    # If the received data is a dictionary, update specific cells or rows
                    for key, value in data.items():
                        setattr(self, key, value)
                else:
                    # If the received data is not a dictionary, update the whole data dictionary
                    for key in self.__dict__:
                        if key in data:
                            setattr(self, key, data[key])




def on_connect(client, userdata, flags, rc):
            print("Connected with result code " + str(rc))
            # Subscribe to the MQTT topic from which you receive the data
            client.subscribe("your/mqtt/topic")

def on_message(client, userdata, message):
            # Handle the incoming MQTT message here and create/update the AutoDiscoveries instance
            discovery = AutoDiscoveries.from_mqtt_message(message)
            # Now you can use 'discovery' instance with the received data
            print("Received data for id:", discovery.id)
            print("MQTT ID:", discovery.mqtt_id)
            # Access other attributes as needed...  
            # Assuming you want to create a DataFrame with the received data
            data_dict = {
                'id': [discovery.id],
                'mqtt_id': [discovery.mqtt_id],
                'device_id': [discovery.device_id],
                'publish_time': [discovery.publish_time],
                'version': [discovery.version],
                'mac_id': [discovery.mac_id],
                'device_type_id': [discovery.device_type_id],
                'sub_type_id': [discovery.sub_type_id],
                'ip_address': [discovery.ip_address],
                'created': [discovery.created],
                'modified': [discovery.modified],
                'oem': [discovery.oem],
                'meter_enabled': [discovery.meter_enabled],
                'sensor_enabled': [discovery.sensor_enabled],
                'override_time': [discovery.override_time],
                'fast_time': [discovery.fast_time],
                'meter_model': [discovery.meter_model],
                'device_ssid': [discovery.device_ssid],
                'device_hw_version': [discovery.device_hw_version],
                'device_serial_no': [discovery.device_serial_no],
                'device_reset_time': [discovery.device_reset_time],
                'device_override_time': [discovery.device_override_time],
                'discoveries_status': [discovery.discoveries_status],
                'status': [discovery.status],
                'slave_id': [discovery.slave_id]
                # Add other attributes as needed...
            }

            # Create a DataFrame from the data dictionary
            df = pd.DataFrame(data_dict)
            # Add any extra columns to the DataFrame
            discovery.add_extra_columns_to_df(df)
            # Now you have a DataFrame containing the received data
            'print(df)'
    # Create the 'data' instance as a global variable
data = dataBases.AutoDiscoveries()
# Configure the MQTT client and callbacks
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
broker_address = "localhost"
broker_port = 1883
client.connect(broker_address, broker_port, 60)

# Start the MQTT client's network loop
client.loop_forever()

  

 
