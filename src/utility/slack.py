""" Module uses to run slack script to generate automated logs when dagsc fail"""
import requests

def read_context_info_from_file(file_path):
    """
    This function reads a text file containing context information and returns a dictionary.

    Parameters:
    file_path (str): The path to the text file containing context information.

    Returns:
    dict: A dictionary where the keys are the context keys and the values
      are the corresponding context values."""
    context_data = {}
    with open(file_path, 'r') as file:
        for line in file:
            if ": " in line:
                key, value = line.strip().split(": ", 1)
                context_data[key] = value
    return context_data

def upload_to_slack():
    """
    This function uploads context information to a Slack channel using a webhook.

    Parameters:
    None

    Returns:
    None
    """
    # Slack webhook URL
    webhook_url = "https://hooks.slack.com/services/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"

    file_path = '/Users/amanpandey/desktop/mock/src/utility/context_info.txt'  # Path where the context file is saved
    context_data = read_context_info_from_file(file_path)
    # Convert context_data to a string with formatted JSON
    formatted_data = [
        {
            "type": "mrkdwn",
            "text": f"*Task Instance:* `{context_data['Task Instance']}`\n"
        },
        {
            "type": "mrkdwn",
            "text": f"*Execution Date:* `{context_data['Execution Date']}`\n"
        },
        {
            "type": "mrkdwn",
            "text": f"*Dag ID:* `{context_data['Dag ID']}`\n"
        },
        {
            "type": "mrkdwn",
            "text": f"*Task ID:* `{context_data['Task ID']}`\n"
        },
        {
            "type": "mrkdwn",
            "text": f"*Run ID:* `{context_data['Run ID']}`\n"
        }
    ]

    # Prepare the payload using Block Kit
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Task Execution Details",
                    "emoji": True
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "For more details, please check the *DAG logs*."
                    }
                ]
            }
        ],
        "attachments": [
            {
                "color": "#36a64f",
                "blocks": [
                    {
                        "type": "section",
                        "fields": formatted_data
                    }
                ]
            }
        ]
    }
   

    # Make the POST request to the Slack webhook URL
    response = requests.post(webhook_url, json=payload,timeout=60)

    # Check if the request was successful
    if response.status_code == 200:
        print("Data uploaded to Slack successfully")
    else:
        print(f"Failed to upload data to Slack. Status code: {response.status_code}")

# Upload the data to Slack

if __name__ == "__main__":
    upload_to_slack()
