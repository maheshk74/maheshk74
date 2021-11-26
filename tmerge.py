## Split and join Twilio data

import pandas as pd

smt = pd.read_csv("smt_output.csv")
smt_in = smt.groupby(smt['direction'])
smt_in = smt_in.get_group("inbound")

smt_out = smt.groupby(smt['direction'])
smt_out = smt_out.get_group("outbound-dial")

smt_merge = pd.merge(smt_in, smt_out, left_on='call_sid', right_on='source_call_sid', suffixes=('_in', '_out'))
smt_merge['combined_talk'] = smt_merge.call_duration_in + smt_merge.call_duration_out
smt_merge = smt_merge.groupby(smt_merge['forwarded_from_out'])
smt_merge = smt_merge.get_group(18558168106)
smt_merge = smt_merge.sort_values(by=['end_time_out'], ascending=True)
smt_merge['CallCount'] = smt_merge.groupby(['call_to_out']).cumcount() + 1
smt_merge['formatted_to'] = smt_merge['call_to_out'].astype(str)
smt_merge['formatted_to'] = smt_merge['formatted_to'].str[-10:]

# smt_merge.to_csv('.\Output\smt_output_merged.csv', index=False)

## Clean decrypted Studymax data

phone = pd.read_csv("sm_output_decr.csv")

phone['CellPhoneNumber'] = phone['CellPhoneNumber'].apply(str)
phone['CellPhoneNumber'] = phone['CellPhoneNumber'].str.replace(" ","")
phone = phone.drop(phone[phone.CellPhoneNumber == "5555555555"].index)

# phone.to_csv('.\Output\sm_output_decr.csv', index=False)

smt_output = pd.merge(smt_merge, phone, left_on='formatted_to', right_on='CellPhoneNumber')
smt_output = smt_output.drop(smt_output.columns[[0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 22, 24]], axis=1)

smt_output.to_csv('.\Output\output.csv', index=False, sep=',', quotechar='"')
