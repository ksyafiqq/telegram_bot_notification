import knime.scripting.io as knio
import requests
import pandas as pd

# Telegram bot details
TOKEN = "xxxx:yyyyyyy"  # Replace with your bot token
chat_id = "-xxxxx"  # Replace with your chat ID
user_tele_id = "xxxxx"

# Function to get the username by sending a dummy message to the user
def get_telegram_username(user_id):
    # You can send a simple message to get the user's interaction
    # Constructing the URL for the Telegram API request
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    
    # Defining the payload to send a simple message
    payload = {
        'chat_id': user_id,
        'text': "Please interact with the bot to get your username"
    }

    # Sending a message to get user interaction
    response = requests.post(url, data=payload)

    # If message was sent successfully, extract username
    if response.status_code == 200:
        # Now the user has interacted, you can extract the username
        # This part assumes you already have the interaction in your bot
        return response.json()["result"]["from"]["username"]
    else:
        print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")
        return None

# Extracting KNIME workflow variables
knime_username = knio.flow_variables["context.workflow.username"]
workflow_name = knio.flow_variables["context.workflow.name"]

# Get Telegram username by sending a message
telegram_username = get_telegram_username(user_tele_id)

# If we successfully get the username, include it in the message
if telegram_username:
    message = f"Workflow {workflow_name} has been completed. Run by {knime_username} and Telegram user @{telegram_username}"
else:
    message = f"Workflow {workflow_name} has been completed. Run by {knime_username}"

# Create an empty Pandas DataFrame
empty_df = pd.DataFrame()

# Set the empty DataFrame as the output table for KNIME
knio.output_tables[0] = knio.Table.from_pandas(empty_df)

# Constructing the URL for the Telegram API request to send the final message
url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
payload = {
    'chat_id': user_tele_id,
    'text': message
}

# Send the final message
response = requests.post(url, data=payload)

# Check if the message was successfully sent
if response.status_code == 200:
    print("Message sent successfully!")
else:
    print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")
