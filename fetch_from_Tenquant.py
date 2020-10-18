import requests
import datetime
import time
import boto3

URL = "https://api.tenquant.io/historical"
key = "eWFqdmFuZUBnbWFpbC5jb20="
request_url_params = {}
test_response_json = "{'assets': 1602827000.0, 'bookvalue': 1014025000.0,'comprehensiveincome': -103033000.0,'comprehensiveincomeattributabletononcontrollinginterest': 0.0,'comprehensiveincomeattributabletoparent': -103033000.0, 'costofrevenue':184465000.0, 'country': 'United States', 'currencycode': 'USD','currentassets': 588002000.0, 'currentliabilities': 90181000.0, 'date':'2019-12-31', 'dividendpayments': 0.0, 'dividendyield': 0.0,'documenttype': '10-K', 'duration': 4, 'equity': 1014025000.0,'exchangegainslosses': 0.0, 'extraordaryitemsgainloss': 0.0, 'grossprofit':-264907000.0, 'incomebeforeequitymethodinvestments': -98864000.0,'incomefromcontinuingoperationsaftertax': -98864000.0,'incomefromcontinuingoperationsbeforetax': -109455000.0,'incomefromequitymethodinvestments': 0.0, 'incometaxexpensebenefit':-10591000.0, 'interestanddebtexpense': 0.0, 'liabilities': 588802000.0,'liabilitiesandequity': 1602827000.0, 'marketcap': 8502960469,'netcashflow': 89976000.0, 'netcashflowsfinancing': 35094000.0,'netcashflowsinvesting': 25013000.0, 'netcashflowsoperating': 29869000.0,'netincomeattributabletononcontrollinginterest': 0.0,'netincomeattributabletoparent': -98864000.0,'netincomeavailabletocommonstockholdersbasic': -98864000.0,'netincomeloss': -98864000.0, 'noncurrentassets': 1014825000.0,'noncurrentliabilities': 498621000.0, 'nonoperatingincomeloss': 0.0,'operatingexpenses': -184465000.0, 'operatingincomeloss': -80442000.0,'othercomprehensiveincome': 4169000.0, 'otheroperatingincome': 0.0, 'pb':8.385355853159439, 'pe': -86.00664012178346, 'preferredstock': 0.0,'preferredstockdividendsandotheradjustments': 0.0, 'price':116.86000061035156, 'revenues': -80442000.0, 'sector': None,'sharesoutstanding': 72761941.0}"

def s3_upload(ticker,year,response_json):
    bucket_name = "tenquant-raw"
    s3 = boto3.resource('s3')
    object = s3.Object(bucket_name,  str(ticker) + "_" + str(year) + "_10-k" )
    #object.put(Body=test_response_json)
    object.put(Body=response_json)


def callTenquantAPI(no_of_years,ticker,year,month,date):
    #For a given Ticker, fetch 10-K for past N years from given date.
    
    for y in range(int(no_of_years)):
        print("Fetch 10-k for year...", year)
        dt = datetime.date(int(year),int(month),int(date))
        #print(dt)
        for d in range(0,366):
            delta=datetime.timedelta(days=d)
            ndate = dt+delta
            ndate = ndate.strftime("%Y%m%d")
            request_url_params["date"]=ndate
            request_url_params["ticker"]=ticker
            request_url_params["key"]=key
            print(URL,request_url_params)
            response = requests.get(url=URL, params=request_url_params)
            if response.status_code == 200:
                data = response.json()
            if "error" in data.keys():
                print("API response was error")
                break
            elif(data["documenttype"] == "10-K"):
                print(response.json())
                s3_upload(ticker,year,str(response.json()))
                break
            else:
                continue
        year-=1


if __name__ == "__main__":
    callTenquantAPI(5,"tdoc",2020,2,15)
    #s3_upload()

