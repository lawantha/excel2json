import os
import pandas as pd
import json
import re
import csv

# Load the CSV file (replace with the actual file path)
folder_path = r"C:\Users\USER\Downloads\oct_17-31"
output_folder_path = r"C:\Users\USER\Downloads\output_AIO\oct_17-31"

os.makedirs(output_folder_path, exist_ok=True)

def extract_file_number(file_name):
    match = re.search(r'\((\d+)\)', file_name)
    return match.group(1) if match else None

# csv_path = r"C:\Users\USER\Downloads\nov 1-2-3\logs-insights-results(1).csv"
# df = pd.read_csv(csv_path)

# Extract only the "message" column
# messages = df['@message'].tolist()
# logStreamName = df['logStreamName'].tolist()

# extract CH_X from logStreamName
def extract_ch(log_stream_name):
    match = re.search(r"CH_(\d+)", log_stream_name)
    if match:
        return f"CH_{match.group(1)}"
    return None

def extract_em(log_stream_name):
    match = re.search(r"EM_(\d+)", log_stream_name)
    if match:
        return f"EM_{match.group(1)}"
    return None

def extract_gwy(log_stream_name):
    match = re.search(r"GWY_(\d+)", log_stream_name)
    if match:
        return f"GWY_{match.group(1)}"
    return None

def define_config(gwy):
    if gwy == "GWY_1" or gwy == "GWY_5":
        return "1Px3"
    else:
        return "3P"

def define_channel(section):
    if section == "P37" or section == "P32" or section == "S31" or section == "S33":
        return "CH_1"
    elif section == "P36" or section == "P33" or section == "S32" or section == "S34":
        return "CH_2"
    elif section == "P38":
        return "CH_3"
    elif section == "Dinning Area" or section == "Kitchen" or section == "Canteen_dining" or section == "Canteen_kitchen" or section == "Library_basement" or section == "Library_GF" or section == "Staff_room" or section == "Auditorium_hall" or section == "Auditorium_blackbox" or section == "Auditorium_lobby" or section == "CEB_supply_2" or section == "CEB_supply_1":
        return "CH_0"
    else:
        return ""

def define_meter(section):
    if section == "P36" or section == "P37" or section == "P38" or section == "S31" or section == "S32" or section == "Dinning Area" or section == "Canteen_dining" or section == "Library_basement" or section == "Library_GF" or section == "Staff_room" or section == "Auditorium_hall" or section == "Auditorium_blackbox" or section == "Auditorium_lobby" or section == "CEB_supply_2" or section == "CEB_supply_1":
        return "EM_1"
    elif section == "P32" or section == "P33" or section == "S33" or section == "S34" or section == "Kitchen" or section == "Canteen_kitchen":
        return "EM_2"
    else:
        return ""

def define_model(config, em):
    if config == "1Px3":
        return "EM4M"
    elif config == "3P" and em == "EM_1":
        return "EM4M"
    else:
        return ""

def filter_message(message_json, field_name, field_type):
    if message_json.get(field_name) is not None:
        filtered_message = {
            "": "",
            "result": "",
            "table": "",
            "_start": "",
            "_stop": "",
            "timestamp": f"{message_json.get('timestamp')}Z".strip(),
            "value": str(message_json.get(field_name)).strip(),
            "_field": field_type.strip() if field_type else "",
            "_measurement": "meter_readings",
            "channel": define_channel(message_json.get("section")).strip(),
            "configuration": define_config(message_json.get("gateway_id")).strip() if message_json.get("gateway_id") else "",
            "description": "3-phase total",
            "gateway_id": str(message_json.get("gateway_id")).strip() if message_json.get("gateway_id") else "",
            "meter_id": define_meter(message_json.get("section")).strip(),
            "model": "PM2120",
}
        return filtered_message
    return None

extra_headers = [
    ["#group", "FALSE", "FALSE", "TRUE", "TRUE", "FALSE", "FALSE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE"],
    ["#datatype", "string", "long", "dateTime:RFC3339", "dateTime:RFC3339", "dateTime:RFC3339", "double", "string", "string", "string", "string", "string", "string", "string", "string"],
    ["#default", "_result", "", "", "", "", "", "", "", "", "", "", "", ""]
]

# Parse message and extract only the needed fields
filtered_data = []
# Loop through each file in the folder
for file_name in os.listdir(folder_path):
    # Only process files that match the pattern "logs-insights-results"
    if file_name.startswith("logs-insights-results") and file_name.endswith(".csv"):

        csv_path = os.path.join(folder_path, file_name)
        
        # Read the CSV file
        df = pd.read_csv(csv_path)

        messages = df['@message'].tolist()

        file_number = extract_file_number(file_name)

        for idx, message in enumerate(messages):
            message_json = json.loads(message)
            # log_stream_name = logStreamName[idx]

            # ch_value = extract_ch(log_stream_name)
            # em_value = extract_em(log_stream_name)

            # energy_message = filter_message(message_json, "energy", "energy_value", ch_value, em_value)
            # if energy_message:
            #     filtered_data.append(energy_message)
            
            power_message = filter_message(message_json, "power", "power_value")
            if power_message:
                filtered_data.append(power_message)
            
            # power_factor_message = filter_message(message_json, "power_factor", "power_factor", ch_value, em_value)
            # if power_factor_message:
            #     filtered_data.append(power_factor_message)

        # Convert to JSON format
        json_data = json.dumps(filtered_data, indent=4)

        # Save JSON data to a file
        output_path = r"C:\Users\USER\Downloads\output.json"
        with open(output_path, 'w') as json_file:
            json_file.write(json_data)

        # Display JSON data
        # print(json_data)

        # Convert to DataFrame
        df = pd.DataFrame(filtered_data)

        # Rename the columns as desired
        df.rename(columns={
            "timestamp": "_time",
            "value": "_value",
            "type": "_feild",
            "channel": "channel_"
        }, inplace=True)

        # Save the DataFrame to a new CSV file
        # csv_output_path = r"C:\Users\USER\Downloads\nov-1-2-3-1.csv"
        # df.to_csv(csv_output_path, index=False)

        if file_number:
            output_csv_path = os.path.join(output_folder_path, f"filtered_data_output({file_number}).csv")
        else:
            output_csv_path = os.path.join(output_folder_path, "filtered_data_output_unknown.csv")

output_csv_path = os.path.join(output_folder_path, "filtered_data_output.csv")
output_df = pd.DataFrame(filtered_data)

        # with open(output_csv_path, mode='w', newline='') as file:
        #     writer = csv.writer(file)

        #     # Write the extra headers as separate rows
        #     for header_row in extra_headers:
        #         writer.writerow(header_row)
            
        #     # Write the actual data (columns) from the DataFrame
        #     df.to_csv(file, index=False, header=True)

output_df = pd.DataFrame(filtered_data)
output_df.to_csv(output_csv_path, index=False)

        # Display the new DataFrame (optional)
        # print(df)

