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


