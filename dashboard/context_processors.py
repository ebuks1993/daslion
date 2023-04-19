import requests
import math
import pandas as pd
import json


## Api base 
api='http://127.0.0.1:8500'


def side(request):
    red=requests.get(f'{api}/semi')
    red1=red.json()
    red2=pd.DataFrame(red1)
    red21=red2[red2['TEAM']=='LION']
    red3=(red21.groupby(['REGION','ASM','CHANNEL','REP'])[['GROUP']].count()).reset_index()
    red31=red3[~(red3["CHANNEL"]=='DISTRIBUTORS')]

    # red4=red3.sort_values(by=red3.columns[0,])
    red4=red31.sort_values(['REGION','ASM','CHANNEL','REP'],ascending=[True,True,True,True])


    jsr=red4.to_json(orient='records')
    dab=[]
    dab=json.loads(jsr)


    qsaf=requests.get(f'{api}/period/')
    qsaf1=qsaf.json()
    qgap=qsaf1[0]['Closing']

    lqsaf=requests.get(f'{api}/period/')
    lqsaf1=lqsaf.json()
    lqgap=lqsaf1[0]['Opening']

    

    return{'red4':dab,'qgap':qgap,'lqgap':lqgap}



   

