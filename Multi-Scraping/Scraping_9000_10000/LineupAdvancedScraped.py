import pandas as pd, numpy as np
import asyncio
import pyppeteer
import time, random
import datetime
#from pyppeteer import launch
from pyppeteer_fork import launch
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from bs4 import BeautifulSoup

# Team ID
id_dict = {}
with open("Team_id.txt", "r") as file:
    for line in file:
        id_dict[line.split("|")[0]] = line.split("|")[1].strip("\n")

# Game Reference Table
ref_table = pd.read_csv("Team_Played_Date.csv")
ref_table["Team_id"] = [id_dict[tm] for tm in ref_table["Team"]]

# Season
def getGameYear(table):
	date_col = table["Date"]
	gameYear = []
	for day in date_col:
		if int(day.split("-")[1]) >= 9:
			gameYear.append(day.split("-")[0][-2:])
		else:
			if int(day.split("-")[0]) <= 2010:
				gameYear.append('0' + str(int(day.split("-")[0][-2:]) - 1))
			else:
				if int(day.split("-")[0]) <= 2010:
					gameYear.append('0' + str(int(day.split("-")[0][-2:]) - 1))
				else:
					gameYear.append(str(int(day.split("-")[0][-2:]) - 1))
	table["Season"] = gameYear
	return table

ref_table = getGameYear(ref_table)

# Regular Season or not
def getRegularSeason(table):
	regularseason = []
	for gameday in table['Date']:
		d = datetime.date(int(gameday.split("-")[0]),
							int(gameday.split("-")[1]),
							int(gameday.split("-")[2]))
		if datetime.date(2006,10,31) <= d <= datetime.date(2007,4,18):
			regularseason.append(1)
		elif datetime.date(2007,10,30) <= d <= datetime.date(2008,4,16):
			regularseason.append(1)
		elif datetime.date(2008,10,28) <= d <= datetime.date(2009,4,16):
			regularseason.append(1)
		elif datetime.date(2009,10,27) <= d <= datetime.date(2010,4,14):
			regularseason.append(1)
		elif datetime.date(2010,10,26) <= d <= datetime.date(2011,4,13):
			regularseason.append(1)
		elif datetime.date(2011,12,25) <= d <= datetime.date(2012,4,26):
			regularseason.append(1)        
		elif datetime.date(2012,10,30) <= d <= datetime.date(2013,4,17):
			regularseason.append(1)    
		elif datetime.date(2013,10,29) <= d <= datetime.date(2014,4,16):
			regularseason.append(1)  
		elif datetime.date(2014,10,28) <= d <= datetime.date(2015,4,15):
			regularseason.append(1)
		elif datetime.date(2015,10,27) <= d <= datetime.date(2016,4,13):
			regularseason.append(1)   
		elif datetime.date(2016,10,25) <= d <= datetime.date(2017,4,12):
			regularseason.append(1)
		elif datetime.date(2017,10,17) <= d <= datetime.date(2018,4,11):
			regularseason.append(1)
		elif datetime.date(2018,10,16) <= d <= datetime.date(2019,4,10):
			regularseason.append(1)
		else:
			regularseason.append(0)
	table["RegularSeason"] = regularseason

	return table[table["RegularSeason"] == 1]

ref_table = getRegularSeason(ref_table)


######################
# Start my Pyppeteer #
######################
async def connect(table, start, end):
	browser = await launch({'headless': False, "devtools": False, 'dumpio':False, 'autoClose':False,'args': ['--no-sandbox'
							]})
	page = await browser.newPage()
	await page.setUserAgent('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36')

	new_table = table.iloc[start:end,:]
	stats = []
	errorGame = []
	errortimes = 0 # if there is too many new tabs, open a new browser

	for ind in range(new_table.shape[0]):
		# Get url information
		spec_table = new_table.iloc[ind, :]
		season = "20" + spec_table["Season"]
		season_next = str(int(season[-2:]) + 1) if int(season[-2:]) + 1 >= 10 else '0' + str(int(season[-2:]) + 1)
		date_format = "%2F".join(spec_table["Date"].split("-"))
		teamid = spec_table["Team_id"]
		url = "https://stats.nba.com/lineups/advanced/?Season="+season+'-'+season_next+"&SeasonType=Regular%20Season&TeamID="+teamid+"&DateFrom="+date_format+"&DateTo="+date_format

		for test in range(4):
			if test >= 3:
				print("Error: " + spec_table["Date"] + "|" + spec_table["Team"])
				errorGame.append(spec_table["Date"] + "|" + spec_table["Team"] + "|" + teamid)
				break
			try:
				# Go to the page
				await page.goto(url)
				print(spec_table["Date"], end  = ": ")

				await page.waitForSelector("body", timeout = 10000)
				html_doc = await page.content()

				# Parse html
				soup = BeautifulSoup(html_doc, "lxml")
				# Extract statistics
				tb = soup.find_all("div", class_="nba-stat-table")[0].find("tbody").find_all("tr")
				print(spec_table["Team"])
				for i in range(tb.__len__()):
					stats.append([ele.text.strip("(\n| |.)+") for ele in tb[i].find_all("td")] + [spec_table["Date"]])


				# Sleep
				await asyncio.sleep(random.randint(3,5))
				break
			except:
				
				if errortimes > 20:
					await browser.close()
					await asyncio.sleep(1)
					browser = await launch({'headless': False,"devtools": False, 'dumpio':False, 'autoClose':False,'args': ['--no-sandbox']})
					page = await browser.newPage()
					await page.setUserAgent('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36')
					
					errortimes = 0
					await asyncio.sleep(random.randint(1,2))
				else:
					page = await browser.newPage()
					await page.setUserAgent('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36')
					await asyncio.sleep(random.randint(1,2))
				errortimes += 1
				#print(errortimes)
		# DataFrame
	df_columns = ['lineup','TEAM','GP','MIN','OFFRTG','DEFRTG','NETRTG','AST%','AST/TO','AST RATIO','OREB%','DREB%','REB%','TO RATIO','EFG%','TS%','PACE','PIE','GAMEDAY']
	table_section = pd.DataFrame(stats, columns = df_columns)

	await browser.close()
	return table_section, errorGame

if __name__ == "__main__":

	start = np.linspace(9000, 9900, 10, dtype = int)
	end = np.linspace(9100, 10000, 10, dtype = int)
	freq = list(zip(start, end))

	for tries in freq:
		try:
			success_table_name = "result_table/table_" + str(tries[0]) + "_" + str(tries[1]) + ".csv"
			fail_text_name = "failed/failed_" + str(tries[0]) + "_" + str(tries[1]) + ".txt"
			time_spend_name = "processing_time/TimeSpend_" + str(tries[0]) + "_" + str(tries[1]) + ".txt"
			
			time_start = time.time()

			#asyncio.get_event_loop().run_until_complete(connect(start, end))
			result = asyncio.run(connect(ref_table, tries[0], tries[1]))

			time_end = time.time()

			#save
			result[0].to_csv(success_table_name, index = False)

			with open(fail_text_name, "w") as file:
				file.write("\n".join(result[1]))

			with open(time_spend_name, "w") as file:
				file.write("Total Processing time is: " + str(time_end - time_start) + " sec.")
		except:
			pass

