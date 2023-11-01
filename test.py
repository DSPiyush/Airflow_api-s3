import requests
import json
import pandas as pd

file_name = 'testFile.csv'
url = https://gutendex.com/books/?page=
def read_a_page(url:str):
    #url = "https://gutendex.com/books/?page=1"
    response = requests.get(url=url)
    # print(f"Response = {response} , {type(response)}")
    json_data = response.json()
    # print("*"*20)
    # print(json_data)
    # print(json_data.keys())
    # We want to only extract the id, title and download_count from the result value 
    list_id = [item['id'] for item in json_data['results']]
    list_title = [item['title'] for item in json_data['results']]
    list_download_count = [item['download_count'] for item in json_data['results']]
    # print(f'{len(list_id)}')
    # print(f'{len(list_title)}')
    # print(f'{len(list_download_count)}')
    df = pd.DataFrame({'id' : list_id, 'title' : list_title, 'download_count' : list_download_count})
    #print(df)
    df.to_csv(f"./{file_name}", sep=',',index=False)