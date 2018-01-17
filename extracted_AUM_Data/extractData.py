import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

## MFID - 3 - Aditya Birla Sun Life Mutual Fund
## MFID - 9 - HDFC Mutual Fund
## MFID - 20 - ICICI Prudential Mutual Fund
## MFID - 22 - SBI Mutual Fund
## MFID - 33 - Sundaram Mutual Fund
## MFID - 25 - Tata Mutual Fund
## MFID - 28 - UTI Mutual Fund
## MFID - 21 - Reliance Mutual Fund
MFID = [3, 9, 20, 22, 33, 25, 28, 21]

## Data is available from March 2012 to December 2017
year= ["5", "4", "3", "2", "1", "0"]

Quarter = ["April - June 201", "July - September 201", "October - December 201", "January - March 201"]


URL = "https://www.amfiindia.com/modules/AverageAUMDetails"
Aumtype = "S"
AumCatType = "Typewise"
for MF_id in MFID:
	headings = ['id', 'scheme', 'Aum-Domestic', 'Aum-Foreign', 'Timeline Quarter']
	df = pd.DataFrame()
	for Year_Id in year:
		for Year_Quarter in Quarter:
			Year_Quarter = Year_Quarter+str(7-int(Year_Id))
			data =  { 'AUmType': Aumtype, 'AumCatType' : AumCatType, 'MF_Id': MF_id, 'Year_Id': Year_Id, 'Year_Quarter': Year_Quarter }
			r = requests.post(url = URL, data = data)

			soup = BeautifulSoup(r.text)
			table = soup.find("table", attrs={"cellpadding":"0"})

			for row in table.find_all("tr")[6:]:
				newrow = [td.get_text() for td in row.find_all("td")]
				if len(newrow)==1:
					break
				if len(newrow)<4:
					continue
				newrow+=[Year_Quarter]
				newrow = pd.DataFrame(np.array(newrow).reshape((1,5)), columns=headings)

				df = df.append(newrow)
			print "Mutual Fund ID:", MF_id, " :: ", Year_Quarter, "completed.."
	df.to_csv('data'+str(MF_id)+'.csv', index=False)
	print 'data', str(MF_id), '.csv created.'

