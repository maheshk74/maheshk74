# Grabs referral ID for matching to Twilio extract
import pandas as pd
import pyodbc

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=studymaxdevops-server.database.windows.net;DATABASE=studymaxprod;UID=dbadmin;PWD=kt@12345678')
#conn2 = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=studymaxdevops-server.database.windows.net;DATABASE=studymaxprod;UID=dbadmin;PWD=kt@12345678')

cursor = conn.cursor
#cursor2 = conn2.cursor

#sql_query = pd.read_sql_query('SELECT ReferralId, CellPhoneNumber FROM Referrals', conn)
#sql_query2 = pd.read_sql_query('SELECT ReferralId, CellPhoneNumber FROM Referrals', conn2)

sql_query = pd.read_sql_query("""SELECT study.Id as StudyID,
	study.StudyTitle as StudyName,
	sd.Name as Screener,
	srr.TimeStamp as ScreeningDateTime,
	(CASE WHEN srr.DQ = 0 THEN 'DQ - Returning Candidate' ELSE sc.Name END) as ReferralResult, 
	referral.ReferralId,
	referral.FirstName,
	referral.LastName,
	referral.EmailAddress,
	referral.CellPhoneNumber,
	referral.HomePhoneNumber,
	referral.Address,
	referral.City,
	referral.State,
	referral.Zip,
	referral.CareGiversName,
	referral.CareGiversPhoneNumber,
	referral.CareGiverEmailAddress
FROM 
	ScreenerReferralRelations srr
		LEFT JOIN ScreenerDetails sd on srr.ScreenerId = sd.Id
			LEFT JOIN Studies study on sd.StudyId = study.Id
		LEFT JOIN ScreenerConclusions sc on srr.DQ = sc.Id
	LEFT JOIN  StudiesReferralsDetails srd on srd.referralid = srr.Id
	LEFT JOIN Referrals referral on srd.ReferralId = referral.ReferralId

WHERE srr.Id IN (SELECT Referralid FROM Screenersavedrecords) AND study.Id NOT IN (1, 17) AND referral.ReferralId IS NOT NULL""", conn)

sql_query2 = pd.read_sql_query("""SELECT study.Id as StudyID,
	study.StudyTitle as StudyName,
	sd.Name as Screener,
	srr.TimeStamp as ScreeningDateTime,
	(CASE WHEN srr.DQ = 0 THEN 'DQ - Returning Candidate' ELSE sc.Name END) as ReferralResult, 
	referral.ReferralId,
	referral.FirstName,
	referral.LastName,
	referral.EmailAddress,
	referral.CellPhoneNumber,
	referral.HomePhoneNumber,
	referral.Address,
	NULL as City,
	NULL as State,
	referral.Zip,
	NULL as CareGiversName,
	NULL as CareGiversPhoneNumber,
	NULL as CareGiverEmailAddress
FROM 
	ScreenerReferralRelations srr
		LEFT JOIN ScreenerDetails sd on srr.ScreenerId = sd.Id
			LEFT JOIN Studies study on sd.StudyId = study.Id
		LEFT JOIN ScreenerConclusions sc on srr.DQ = sc.Id
	--LEFT JOIN  StudiesReferralsDetails srd on srd.referralid = srr.Id
	LEFT JOIN ScreenerTempContacts referral on srr.Id = referral.ReferralId

WHERE referral.ReferralId NOT IN (SELECT Referralid FROM Referrals) AND study.Id NOT IN (1, 17) AND referral.ReferralId IS NOT NULL""", conn)

output = sql_query.append(sql_query2, ignore_index=True)

#output['ReferralId'] = output['ReferralId'].astype(int)

#output['HomePhoneNumber'] = output['HomePhoneNumber'].str.replace("NULL","")

smarts_output = output[["ReferralId", "CellPhoneNumber"]]

smarts_output.to_csv('sm_output.csv', index=False)
output.to_csv('pg_output.csv', index=False)
