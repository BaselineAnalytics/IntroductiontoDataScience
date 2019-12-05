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

######################
# Start my Pyppeteer #
######################

async def connect(new_table):
	browser = await launch({'headless': False, "devtools": False, 'dumpio':False, 'autoClose':False,'args': ['--no-sandbox'
							]})
	page = await browser.newPage()
	await page.setUserAgent('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36')

	stats = []
	errorGame = []
	errortimes = 0 # if there is too many new tabs, open a new browser

	for ind in range(new_table.shape[0]):
		# Get url information
		spec_table = new_table.iloc[ind, :]

		season = spec_table["Season"]
		date_format = "%2F".join(spec_table["Date"].split("-"))
		teamid = spec_table["Team_id"]

		url = "https://stats.nba.com/lineups/traditional/?Season="+season+"&SeasonType=Regular%20Season&TeamID="+teamid+"&DateFrom="+date_format+"&DateTo="+date_format

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
				
				if errortimes > 12:
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
	df_columns = ['lineup','TEAM','GP','MIN','PTS','FGM','FGA','FG%','3PM',
					'3PA','3P%','FTM','FTA','FT%','OREB','DREB','REB',
					'AST','TOV','STL','BLK','BLKA','PF','PFD','+/-','GAMEDAY']
	table_section = pd.DataFrame(stats)

	#await browser.close()
	return table_section, errorGame

if __name__ == "__main__":
	
	ref_table = pd.read_csv("Team_Played_Date.csv")

	#asyncio.get_event_loop().run_until_complete(connect(start, end))
	result = asyncio.run(connect(ref_table))

	success_table_name = "rescraping.csv"
	fail_text_name = "failed_again.txt"

	#save
	result[0].to_csv(success_table_name, index = False)

	with open(fail_text_name, "w") as file:
		file.write("\n".join(result[1]))


