
### 1. All the packages you need

- pip install pandas
- pip install numpy
- pip install asyncio
- pip install pyppeteer
- pip install random
- pip install time
- pip install datetime
- pip install pyppeteer_fork
- pip install bs4

### 2. 如何操作

- Step1: 下載zip file，然後解壓縮（應該會自己解壓縮）

- Step2: 打開 Mac Terminal，Change Directory，舉例： cd /Users/garyliu/Downloads/Scraping_6000_7000

- Step3: 先 pip install 上方所有套件

- Step4: 輸入：python3 -i LineupAdvancedScraped.py

- P.S. 有成功在跑的話，會顯示日期、冒號、隊伍：e.g. **2019-01-17: NYK**；失敗的隊伍會顯示：e.g. **Error: 2019-01-17|WAS**



### 3. 注意事項

- <font color=red>**重要：請把Mac節能模式設定成「永不」！！！！！！！！！！！！**</font>

- 爬蟲檔案以每1000, 2000, 2500筆作為區分，從檔名可得知

- 這個方法還不是很穩定，chrome會不定時跳出來，所以會影響你打字，需要大量打字時，就先別跑吧！
- 1000筆大致需花上一個半小時 ~ 一個小時又45分鐘

### 4. 建議


- 任何讓電腦「休眠」的狀況都會 disconnect 爬蟲，記得估計好有空閒的時間

- <font color=red>若覺得自動重開瀏覽器次數太頻繁，請到 LineupAdvancedScraped.py 第129行，把 errortimes > 12 改成 errortimes > 20 or errortimes > 25 之類的，這樣會增加每次打開瀏覽器的 New Tab 數量</font>