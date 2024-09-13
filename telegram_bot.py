import knime.scripting.io as knio
import requests
import pandas as pd

# Telegram bot details
TOKEN = "xxxx:yyy" #telegram token
chat_id = "-hehe" #chat id/ chat group id
message = "Nodes dah siap run mattt"

# Create an empty Pandas DataFrame
empty_df = pd.DataFrame()

# Set the empty DataFrame as the output table
knio.output_tables[0] = knio.Table.from_pandas(empty_df)

# Constructing the URL for the Telegram API request
url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"

# Send the request and handle the response
response = requests.get(url)

# Check if the message was successfully sent
if response.status_code == 200:
    print("Message sent successfully!")
else:
    print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")

