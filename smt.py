# Download the helper library from https://www.twilio.com/docs/python/install
import pandas as pd
import os
from twilio.rest import Client

# Your Account Sid and Auth Token from twilio.com/console
# and set the environment variables. See http://twil.io/secure
# account_sid = os.environ['TWILIO_ACCOUNT_SID']
account_sid = 'AC0206bbb7cde32424c726ab9979825837'
# auth_token = os.environ['TWILIO_AUTH_TOKEN']
auth_token = 'a1bad989c635b025e05c5fe285e43bb3'

client = Client(account_sid, auth_token)

# calls = client.calls.list(limit=20)
calls = client.calls.list()

# calls

# callsdf = pd.DataFrame(data = calls.transpose(), columns = ['call_sid', 'parent_call_sid', 'from_', 'to', 'duration', 'direction', 'start_time', 'end_time', 'status', 'forwarded_from'])
# callsdf

with open('smarts_output.csv', 'w') as f:
    print('call_sid,source_call_sid,call_from,call_to,call_duration,direction,start_time,end_time,status,forwarded_from', file = f)
    for record in calls:
        print(record.sid, record.parent_call_sid, record.from_, record.to, record.duration, record.direction, record.start_time, record.end_time, record.status, record.forwarded_from, sep = ",", file = f)
