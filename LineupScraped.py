import pandas as pd, numpy as np
import asyncio
import pyppeteer
import time, random
from pyppeteer import launch
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
def getGameYear(date_col):
    gameYear = []
    for day in date_col:
        if int(day.split("-")[1]) >= 9:
            gameYear.append(day.split("-")[0][-2:])
        else:
            gameYear.append(str(int(day.split("-")[0][-2:]) - 1))
    return gameYear

ref_table["Season"] = getGameYear(ref_table["Date"])


######################
# Start my Pyppeteer #
######################
async def connect(start, end):
	browser = await launch({'headless': False, "devtools": False, 'dumpio':False, 'autoClose':False,'args': ['--no-sandbox', '--window-size=1366,850']})
	page = await browser.newPage()
	await page.setUserAgent('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36')

	new_table = ref_table.iloc[start:end,:]
	stats = []
	errorGame = []
	for ind in range(new_table.shape[0]):
		# Get url information
		spec_table = new_table.iloc[ind, :]
		season = "20" + spec_table["Season"]
		season_next = str(int(season[-2:]) + 1) if int(season[-2:]) + 1 >= 10 else '0' + str(int(season[-2:]) + 1)
		date_format = "%2F".join(spec_table["Date"].split("-"))
		teamid = spec_table["Team_id"]
		url = "https://stats.nba.com/lineups/traditional/?Season="+season+'-'+season_next+"&SeasonType=Regular%20Season&TeamID="+teamid+"&DateFrom="+date_format+"&DateTo="+date_format

		for test in range(3):
			if test >= 2:
				print("Error: " + spec_table["Date"] + "|" + spec_table["Team"])
				errorGame.append(spec_table["Date"] + "|" + spec_table["Team"] + "|" + teamid)
			try:
				# Go to the page
				await page.goto(url)
				print("success")

				await page.waitForSelector("body", timeout = 10000)
				html_doc = await page.content()

				# Parse html
				soup = BeautifulSoup(html_doc, "lxml")
				# Extract statistics
				tb = soup.find_all("div", class_="nba-stat-table")[0].find("tbody").find_all("tr")

				for i in range(tb.__len__()):
					stats.append([ele.text.strip("(\n| |.)+") for ele in tb[i].find_all("td")] + [spec_table["Date"]])


				# Sleep
				await asyncio.sleep(random.randint(2,5))
				break
			except:
				#await page.goto('about:blank')
				await asyncio.sleep(1)

				await browser.close()

				browser = await launch({'headless': False,"devtools": False, 'dumpio':False, 'autoClose':False,'args': ['--no-sandbox', '--window-size=1366,850']})
				page = await browser.newPage()
				await page.setUserAgent('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36')
				await asyncio.sleep(random.randint(1,2))
		# DataFrame
	df_columns = ['lineup','TEAM','GP','MIN','PTS','FGM','FGA','FG%','3PM',
					'3PA','3P%','FTM','FTA','FT%','OREB','DREB','REB',
					'AST','TOV','STL','BLK','BLKA','PF','PFD','+/-','GAMEDAY']
	table_section = pd.DataFrame(stats)

	#await browser.close()
	return table_section, errorGame

if __name__ == "__main__":
	time_start = time.time()

	start = 0
	end = 10
	success_table_name = "table_" + str(start) + "_" + str(end) + ".csv"
	fail_text_name = "failed_" + str(start) + "_" + str(end) + ".txt"
	time_spend_name = "TimeSpend_" + str(start) + "_" + str(end) + ".txt"
	
	#asyncio.get_event_loop().run_until_complete(connect(start, end))
	result = asyncio.run(connect(start, end))

	time_end = time.time()

	#save
	result[0].to_csv(success_table_name, index = False)

	with open(fail_text_name, "w") as file:
		file.write("\n".join(result[1]))

	with open(time_spend_name, "w") as file:
		file.write("Total Processing time is: " + str(time_end - time_start) + " sec.")