from django.shortcuts import render
import pandas as pd
import json
import requests
import datetime

##API
api='http://salesb12-production.up.railway.app'

#TEAM
kapu='LION'

## DATE PERIOD
qsaf=requests.get(f'{api}/period/')
qsaf1=qsaf.json()
qgap=qsaf1[0]['Closing']
qdap=qsaf1[0]['Closing']
qdap1=qdap.split('-')
qdap1[0]=str(int(qdap1[0])-1)
qdap2='-'.join(qdap1)


lqsaf=requests.get(f'{api}/period/')
lqsaf1=lqsaf.json()
lqgap=lqsaf1[0]['Opening']
lqdap=lqsaf1[0]['Opening']
lqdap1=lqdap.split('-')
lqdap1[0]=str(int(lqdap1[0])-1)
lqdap2='-'.join(lqdap1)


## MONTH PERIOD
Rqsaf=requests.get(f'{api}/period/')
Rqsaf1=Rqsaf.json()
Rqgap=Rqsaf1[0]['Closing']
Rqdap=Rqsaf1[0]['Closing']
Rqdap1=Rqdap.split('-')
Rqdap1[2]=str(int(Rqdap1[2])-(int(Rqdap1[2])-1))
Rqdap2='-'.join(Rqdap1)


## budget period 

rwe=requests.get(f'{api}/Month/?fullstatus=Active')
rwe1=rwe.json()
rwe2=rwe1[0]['pointer']

## month period
sunk=pd.DataFrame({'Month':['April','May','June',"July",'August','September','October','November','December','January','Febuary','March'],'num':[4,5,6,7,8,9,10,11,12,1,2,3]})





def kokun(request,rep,asm):
    sup=str(rep)
    dam=sup.split(' ')
    tam='+'.join(dam)

    sup1=str(asm)
    dam1=sup1.split(' ')
    tam1='+'.join(dam1)



    eba=requests.get(f'{api}/semi/?REGION=&CHANNEL=&TEAM={kapu}&ASM={tam1}&REP={tam}')
    eba1= eba.json()
    eba2=pd.DataFrame(eba1)
    eba3=eba2['id'].tolist()

    name1=eba1[0]['GROUP']

    pr=requests.get(f"{api}/collection/?Date__gte={lqgap}&Date__lte={qgap}")

    ## year to date
    pr1=pr.json()
    pr2=pd.DataFrame(pr1)
    pr22=pr2[pr2['semi'].isin(eba3)]
    pr3=(pr22['Amount'].sum())/1000000

    ## Last year
    # prm=requests.get(f'{api}/Prevcollection/?Date__gte={lqdap2}&Date__lte={qdap2}')
    # prm1=prm.json()
    # prm2=pd.DataFrame(prm1)
    # prm22=prm2[prm2['semi'].isin(eba3)]
    # prm3=(prm22['Amount'].sum())/1000000


    opy=requests.get(f"{api}/Budget/?procode=&TEAM={kapu}")
    opy1=opy.json()
    opy2=pd.DataFrame(opy1)
    opy3=opy2[opy2['semi'].isin(eba3)]
    opy4=((opy3['buz'].sum())/1000000)*rwe2
    opy5=((opy3['buz'].sum())/1000000)


    ## ytd collection growth
    # colgrowth=(pr3-prm3)/prm3

    ## ytd budget achiived
    sca=(pr3/opy4)*100


    ##MTD for collection
    ##current
    # mpr=pr22[(pr22['Date']<=Rqdap)&(pr22['Date']>=Rqdap2)]
    # # mpr2=(mpr['Amount'].sum())/1000000

    mpr=requests.get(f"{api}/collection/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    mpra=mpr.json()
    mprb=pd.DataFrame(mpra)
    mprc=mprb[mprb['semi'].isin(eba3)]
    mpr2=(mprc['Amount'].sum())/1000000

    ## previous


    ## mtd budget achiived for collection
    mcba=(mpr2/opy5)*100






    spr=requests.get(f'{api}/current/?TEAM=&Date__gte={lqgap}&Date__lte={qgap}&pro=')

    ## year to date
    spr1=spr.json()
    spr2=pd.DataFrame(spr1)
    spr22=spr2[spr2['semi'].isin(eba3)]
    spr3=(spr22['Money'].sum())/1000000

    ## Last year
    sprm=requests.get(f'{api}/Previous/?Date__gte={lqdap2}&Date__lte={qdap2}')
    sprm1=sprm.json()
    sprm2=pd.DataFrame(sprm1)
    sprm22=sprm2[sprm2['semi'].isin(eba3)]
    sprm3=(sprm22['Money'].sum())/1000000


    ##MTD forsales
    ##current for sales
    smpr=requests.get(f"{api}/current/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    smpra=smpr.json()
    smprb=pd.DataFrame(smpra)
    smprc=smprb[smprb['semi'].isin(eba3)]
    smpr2=(smprc['Money'].sum())/1000000

    ## previous


    ## mtd budget achiived for sales
    smcba=(smpr2/opy5)*100





    ## sales growth
    salgrowth=((spr3-sprm3)/sprm3)*100

    ## budget achiived
    sba=(spr3/opy4)*100



    finta={'curcol':pr3,'ytdbudget':opy4,'mtdbudget':opy5,'mcol':mpr2,'mcba':mcba,'sca':sca,'cursales':spr3,
        'lastsales':sprm3,'salgrowth':salgrowth,'sba':sba,'msal':smpr2,'msa':smcba}
    

    # ______________________________________________________________________


        ## ytd
    prod1=(spr22.groupby(['pro','product'])[['ctns']].sum()).reset_index()
    prod2=(sprm22.groupby(['pro'])[['ctns']].sum()).reset_index()
    prod3=(opy3.groupby(['procode'])[['value']].sum()).reset_index()


    ## merge the ytd records 
    prod4=pd.merge(prod1,prod2,on='pro',how='left')
    prod5=pd.merge(prod4,prod3,left_on='pro',right_on='procode',how='left')
    prod5['tgt']=prod5['value']*rwe2

    ## mtd
    rod1=(smprc.groupby(['pro','product'])[['ctns']].sum()).reset_index()


    prod6=pd.merge(prod5,rod1,on='pro',how='left')
    prod7=prod6.fillna(0)


    ##get the percentages
    prod7['ytdA']=(prod7['ctns_x']/prod7['tgt'])*100
    prod7['mtdA']=(prod7['ctns']/prod7['value'])*100
    prod7['gwt']=((prod7['ctns_x']-prod7['ctns_y'])/prod7['ctns_y'])*100

    rcbsr=prod7.to_json(orient='records')
    rcfab=[]
    rcfab=json.loads(rcbsr)
    rcfab



    #-------------------------TREND CHART------------------------------------------

    # lprm=requests.get(f'{api}/Prevcollection/?Date__gte=&Date__lte=')
    # lprm1=lprm.json()
    # lprm2=pd.DataFrame(lprm1)
    # lprm22=lprm2[lprm2['semi'].isin(eba3)]
    # lprm22['num']=pd.DatetimeIndex(lprm22['Date']).month
    # cur2=lprm22.groupby(['num'])['Amount'].sum()

    pr23=pr22.copy()
    pr23['num']=pd.DatetimeIndex(pr23['Date']).month
    cur1=pr23.groupby(['num'])[['Amount']].sum()


    ## merge all
    ped1=pd.merge(sunk,cur1,on='num',how='left')
    # ped2=pd.merge(ped1,cur2,on='num',how='left')

    ped3=ped1.fillna(0)

    ped3['budget']=opy5
    ped3['Amount_x1']=ped3['Amount']/1000000
    # ped3['Amount_y1']=ped3['Amount_y']/1000000


    trcbsr=ped3.to_json(orient='records')
    trcfab=[]
    trcfab=json.loads(trcbsr)
    trcfab


    #--------------------------------Last 7 days collection-------------------------------

    
    y=qgap
    y1=datetime.datetime.strptime(y,'%Y-%m-%d')
    y9=y1.strftime("%Y-%m-%d")
    y10=y1.strftime("%w")
    y11=y1.strftime('%A')
    cin=[y9]
    gol=[y10]
    zol=[y11]





    for i in range(1,20):
        y2=y1 - datetime.timedelta(days=1)
        y7=y2.strftime("%Y-%m-%d")
        y3=y2.strftime('%w')
        yin=y2.strftime('%A')
        if y3=='0' or y3=='6':
            pass
        else:
            cin.append(y7)
            gol.append(y3)
            zol.append(yin)
        if len(cin)>4:
            break
        y1=y2



    lad=pd.DataFrame({'dated':cin,'nos':gol,'day':zol})





    lad['dated']=lad['dated'].astype('datetime64[ns]')
    pr29=pr22.copy()
    pr29['Date']=pr29['Date'].astype('datetime64[ns]')
    pr2023=pr29.groupby(['Date'])[['Amount']].sum()

    parf=pd.merge(lad,pr2023,left_on='dated',right_on='Date',how='left')
    parf2=parf.fillna(0)
    parf2['dated']=parf2['dated'].astype(str)

    qrcbsr=parf2.to_json(orient='records')
    qrcfab=[]
    qrcfab=json.loads(qrcbsr)
    qrcfab

    

    context= {'finta':finta,'name1':name1,'rcfab':rcfab,'trcfab':trcfab,'qrcfab':qrcfab}

    return render(request,'dash/rep.html',context)





def lotus(request):
    return render(request,'dash/pen.html')

def potus(request):
    return render(request,'dash/zen.html')




def area (request,asm,reg):
    # sup=str(rep)
    # dam=sup.split(' ')
    # tam='+'.join(dam)

    sup1=str(asm)
    dam1=sup1.split(' ')
    tam1='+'.join(dam1)



    eba=requests.get(f'{api}/semi/?REGION={reg}&CHANNEL=&TEAM={kapu}&ASM={tam1}&REP=')
    eba1= eba.json()
    eba2=pd.DataFrame(eba1)
    eba3=eba2['id'].tolist()

    name1=asm

    pr=requests.get(f"{api}/collection/?Date__gte={lqgap}&Date__lte={qgap}")

    ## year to date
    pr1=pr.json()
    pr2=pd.DataFrame(pr1)
    pr22=pr2[pr2['semi'].isin(eba3)]
    pr3=(pr22['Amount'].sum())/1000000

    ## Last year
    # prm=requests.get(f'{api}/Prevcollection/?Date__gte={lqdap2}&Date__lte={qdap2}')
    # prm1=prm.json()
    # prm2=pd.DataFrame(prm1)
    # prm22=prm2[prm2['semi'].isin(eba3)]
    # prm3=(prm22['Amount'].sum())/1000000


    opy=requests.get(f"{api}/Budget/?procode=&TEAM={kapu}")
    opy1=opy.json()
    opy2=pd.DataFrame(opy1)
    opy3=opy2[opy2['semi'].isin(eba3)]
    opy4=((opy3['buz'].sum())/1000000)*rwe2
    opy5=((opy3['buz'].sum())/1000000)


    ## ytd collection growth
    # colgrowth=(pr3-prm3)/prm3

    ## ytd budget achiived
    sca=(pr3/opy4)*100


    ##MTD for collection
    ##current
    # mpr=pr22[(pr22['Date']<=Rqdap)&(pr22['Date']>=Rqdap2)]
    # mpr2=(mpr['Amount'].sum())/1000000
    mpr=requests.get(f"{api}/collection/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    mpra=mpr.json()
    mprb=pd.DataFrame(mpra)
    mprc=mprb[mprb['semi'].isin(eba3)]
    mpr2=(mprc['Amount'].sum())/1000000

    ## previous


    ## mtd budget achiived for collection
    mcba=(mpr2/opy5)*100






    spr=requests.get(f'{api}/current/?TEAM=&Date__gte={lqgap}&Date__lte={qgap}&pro=')

    ## year to date
    spr1=spr.json()
    spr2=pd.DataFrame(spr1)
    spr22=spr2[spr2['semi'].isin(eba3)]
    spr3=(spr22['Money'].sum())/1000000

    ## Last year
    sprm=requests.get(f'{api}/Previous/?Date__gte={lqdap2}&Date__lte={qdap2}')
    sprm1=sprm.json()
    sprm2=pd.DataFrame(sprm1)
    sprm22=sprm2[sprm2['semi'].isin(eba3)]
    sprm3=(sprm22['Money'].sum())/1000000


    ##MTD forsales
    ##current for sales
    smpr=requests.get(f"{api}/current/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    smpra=smpr.json()
    smprb=pd.DataFrame(smpra)
    smprc=smprb[smprb['semi'].isin(eba3)]
    smpr2=(smprc['Money'].sum())/1000000

    ## previous


    ## mtd budget achiived for sales
    smcba=(smpr2/opy5)*100





    ## sales growth
    salgrowth=((spr3-sprm3)/sprm3)*100

    ## budget achiived
    sba=(spr3/opy4)*100



    finta={'curcol':pr3,'ytdbudget':opy4,'mtdbudget':opy5,'mcol':mpr2,'mcba':mcba,'sca':sca,'cursales':spr3,
        'lastsales':sprm3,'salgrowth':salgrowth,'sba':sba,'msal':smpr2,'msa':smcba}
    

    # ______________________________________________________________________


        ## ytd
    prod1=(spr22.groupby(['pro','product'])[['ctns']].sum()).reset_index()
    prod2=(sprm22.groupby(['pro'])[['ctns']].sum()).reset_index()
    prod3=(opy3.groupby(['procode'])[['value']].sum()).reset_index()


    ## merge the ytd records 
    prod4=pd.merge(prod1,prod2,on='pro',how='left')
    prod5=pd.merge(prod4,prod3,left_on='pro',right_on='procode',how='left')
    prod5['tgt']=prod5['value']*rwe2

    ## mtd
    rod1=(smprc.groupby(['pro','product'])[['ctns']].sum()).reset_index()


    prod6=pd.merge(prod5,rod1,on='pro',how='left')
    prod7=prod6.fillna(0)


    ##get the percentages
    prod7['ytdA']=(prod7['ctns_x']/prod7['tgt'])*100
    prod7['mtdA']=(prod7['ctns']/prod7['value'])*100
    prod7['gwt']=((prod7['ctns_x']-prod7['ctns_y'])/prod7['ctns_y'])*100

    rcbsr=prod7.to_json(orient='records')
    rcfab=[]
    rcfab=json.loads(rcbsr)
    rcfab



    #-------------------------TREND CHART------------------------------------------

    # lprm=requests.get(f'{api}/Prevcollection/?Date__gte=&Date__lte=')
    # lprm1=lprm.json()
    # lprm2=pd.DataFrame(lprm1)
    # lprm22=lprm2[lprm2['semi'].isin(eba3)]
    # lprm22['num']=pd.DatetimeIndex(lprm22['Date']).month
    # cur2=lprm22.groupby(['num'])['Amount'].sum()

    pr23=pr22.copy()
    pr23['num']=pd.DatetimeIndex(pr23['Date']).month
    cur1=pr23.groupby(['num'])[['Amount']].sum()


    ## merge all
    ped1=pd.merge(sunk,cur1,on='num',how='left')
    # ped2=pd.merge(ped1,cur2,on='num',how='left')

    ped3=ped1.fillna(0)

    ped3['budget']=opy5
    ped3['Amount_x1']=ped3['Amount']/1000000
    # ped3['Amount_y1']=ped3['Amount_y']/1000000


    trcbsr=ped3.to_json(orient='records')
    trcfab=[]
    trcfab=json.loads(trcbsr)
    trcfab


    #--------------------------------Last 7 days collection-------------------------------

    
    y=qgap
    y1=datetime.datetime.strptime(y,'%Y-%m-%d')
    y9=y1.strftime("%Y-%m-%d")
    y10=y1.strftime("%w")
    y11=y1.strftime('%A')
    cin=[y9]
    gol=[y10]
    zol=[y11]





    for i in range(1,20):
        y2=y1 - datetime.timedelta(days=1)
        y7=y2.strftime("%Y-%m-%d")
        y3=y2.strftime('%w')
        yin=y2.strftime('%A')
        if y3=='0' or y3=='6':
            pass
        else:
            cin.append(y7)
            gol.append(y3)
            zol.append(yin)
        if len(cin)>4:
            break
        y1=y2



    lad=pd.DataFrame({'dated':cin,'nos':gol,'day':zol})





    lad['dated']=lad['dated'].astype('datetime64[ns]')
    pr29=pr22.copy()
    pr29['Date']=pr29['Date'].astype('datetime64[ns]')
    pr2023=pr29.groupby(['Date'])[['Amount']].sum()

    parf=pd.merge(lad,pr2023,left_on='dated',right_on='Date',how='left')
    parf2=parf.fillna(0)
    parf2['dated']=parf2['dated'].astype(str)

    qrcbsr=parf2.to_json(orient='records')
    qrcfab=[]
    qrcfab=json.loads(qrcbsr)
    qrcfab

    

    context= {'finta':finta,'name1':name1,'rcfab':rcfab,'trcfab':trcfab,'qrcfab':qrcfab}

    return render(request,'dash/area.html',context)
















def region (request,reg):
    # sup=str(rep)
    # dam=sup.split(' ')
    # tam='+'.join(dam)

    # sup1=str(asm)
    # dam1=sup1.split(' ')
    # tam1='+'.join(dam1)



    eba=requests.get(f'{api}/semi/?REGION={reg}&CHANNEL=&TEAM={kapu}&ASM=&REP=&SEGMENT=Marketing')
    eba1= eba.json()
    eba2=pd.DataFrame(eba1)
    eba3=eba2['id'].tolist()

    name1=reg

    pr=requests.get(f"{api}/collection/?Date__gte={lqgap}&Date__lte={qgap}")

    ## year to date
    pr1=pr.json()
    pr2=pd.DataFrame(pr1)
    pr22=pr2[pr2['semi'].isin(eba3)]
    pr3=(pr22['Amount'].sum())/1000000

    ## Last year
    # prm=requests.get(f'{api}/Prevcollection/?Date__gte={lqdap2}&Date__lte={qdap2}')
    # prm1=prm.json()
    # prm2=pd.DataFrame(prm1)
    # prm22=prm2[prm2['semi'].isin(eba3)]
    # prm3=(prm22['Amount'].sum())/1000000


    opy=requests.get(f"{api}/Budget/?procode=&TEAM={kapu}")
    opy1=opy.json()
    opy2=pd.DataFrame(opy1)
    opy3=opy2[opy2['semi'].isin(eba3)]
    opy4=((opy3['buz'].sum())/1000000)*rwe2
    opy5=((opy3['buz'].sum())/1000000)


    ## ytd collection growth
    # colgrowth=(pr3-prm3)/prm3

    ## ytd budget achiived
    sca=(pr3/opy4)*100


    ##MTD for collection
    ##current
    # mpr=pr22[(pr22['Date']<=Rqdap)&(pr22['Date']>=Rqdap2)]
    # mpr2=(mpr['Amount'].sum())/1000000

    mpr=requests.get(f"{api}/collection/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    mpra=mpr.json()
    mprb=pd.DataFrame(mpra)
    mprc=mprb[mprb['semi'].isin(eba3)]
    mpr2=(mprc['Amount'].sum())/1000000

    ## previous


    ## mtd budget achiived for collection
    mcba=(mpr2/opy5)*100






    spr=requests.get(f'{api}/current/?TEAM=&Date__gte={lqgap}&Date__lte={qgap}&pro=')

    ## year to date
    spr1=spr.json()
    spr2=pd.DataFrame(spr1)
    spr22=spr2[spr2['semi'].isin(eba3)]
    spr3=(spr22['Money'].sum())/1000000

    ## Last year
    sprm=requests.get(f'{api}/Previous/?Date__gte={lqdap2}&Date__lte={qdap2}')
    sprm1=sprm.json()
    sprm2=pd.DataFrame(sprm1)
    sprm22=sprm2[sprm2['semi'].isin(eba3)]
    sprm3=(sprm22['Money'].sum())/1000000


    ##MTD forsales
    ##current for sales
    smpr=requests.get(f"{api}/current/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    smpra=smpr.json()
    smprb=pd.DataFrame(smpra)
    smprc=smprb[smprb['semi'].isin(eba3)]
    smpr2=(smprc['Money'].sum())/1000000

    ## previous


    ## mtd budget achiived for sales
    smcba=(smpr2/opy5)*100





    ## sales growth
    salgrowth=((spr3-sprm3)/sprm3)*100

    ## budget achiived
    sba=(spr3/opy4)*100



    finta={'curcol':pr3,'ytdbudget':opy4,'mtdbudget':opy5,'mcol':mpr2,'mcba':mcba,'sca':sca,'cursales':spr3,
        'lastsales':sprm3,'salgrowth':salgrowth,'sba':sba,'msal':smpr2,'msa':smcba}
    

    # ______________________________________________________________________


        ## ytd
    prod1=(spr22.groupby(['pro','product'])[['ctns']].sum()).reset_index()
    prod2=(sprm22.groupby(['pro'])[['ctns']].sum()).reset_index()
    prod3=(opy3.groupby(['procode'])[['value']].sum()).reset_index()


    ## merge the ytd records 
    prod4=pd.merge(prod1,prod2,on='pro',how='left')
    prod5=pd.merge(prod4,prod3,left_on='pro',right_on='procode',how='left')
    prod5['tgt']=prod5['value']*rwe2

    ## mtd
    rod1=(smprc.groupby(['pro','product'])[['ctns']].sum()).reset_index()


    prod6=pd.merge(prod5,rod1,on='pro',how='left')
    prod7=prod6.fillna(0)


    ##get the percentages
    prod7['ytdA']=(prod7['ctns_x']/prod7['tgt'])*100
    prod7['mtdA']=(prod7['ctns']/prod7['value'])*100
    prod7['gwt']=((prod7['ctns_x']-prod7['ctns_y'])/prod7['ctns_y'])*100

    rcbsr=prod7.to_json(orient='records')
    rcfab=[]
    rcfab=json.loads(rcbsr)
    rcfab



    #-------------------------TREND CHART------------------------------------------

    # lprm=requests.get(f'{api}/Prevcollection/?Date__gte=&Date__lte=')
    # lprm1=lprm.json()
    # lprm2=pd.DataFrame(lprm1)
    # lprm22=lprm2[lprm2['semi'].isin(eba3)]
    # lprm22['num']=pd.DatetimeIndex(lprm22['Date']).month
    # cur2=lprm22.groupby(['num'])['Amount'].sum()

    pr23=pr22.copy()
    pr23['num']=pd.DatetimeIndex(pr23['Date']).month
    cur1=pr23.groupby(['num'])[['Amount']].sum()


    ## merge all
    ped1=pd.merge(sunk,cur1,on='num',how='left')
    # ped2=pd.merge(ped1,cur2,on='num',how='left')

    ped3=ped1.fillna(0)

    ped3['budget']=opy5
    ped3['Amount_x1']=ped3['Amount']/1000000
    # ped3['Amount_y1']=ped3['Amount_y']/1000000


    trcbsr=ped3.to_json(orient='records')
    trcfab=[]
    trcfab=json.loads(trcbsr)
    trcfab


    #--------------------------------Last 7 days collection-------------------------------

    
    y=qgap
    y1=datetime.datetime.strptime(y,'%Y-%m-%d')
    y9=y1.strftime("%Y-%m-%d")
    y10=y1.strftime("%w")
    y11=y1.strftime('%A')
    cin=[y9]
    gol=[y10]
    zol=[y11]





    for i in range(1,20):
        y2=y1 - datetime.timedelta(days=1)
        y7=y2.strftime("%Y-%m-%d")
        y3=y2.strftime('%w')
        yin=y2.strftime('%A')
        if y3=='0' or y3=='6':
            pass
        else:
            cin.append(y7)
            gol.append(y3)
            zol.append(yin)
        if len(cin)>4:
            break
        y1=y2



    lad=pd.DataFrame({'dated':cin,'nos':gol,'day':zol})





    lad['dated']=lad['dated'].astype('datetime64[ns]')
    pr29=pr22.copy()
    pr29['Date']=pr29['Date'].astype('datetime64[ns]')
    pr2023=pr29.groupby(['Date'])[['Amount']].sum()

    parf=pd.merge(lad,pr2023,left_on='dated',right_on='Date',how='left')
    parf2=parf.fillna(0)
    parf2['dated']=parf2['dated'].astype(str)

    qrcbsr=parf2.to_json(orient='records')
    qrcfab=[]
    qrcfab=json.loads(qrcbsr)
    qrcfab

    

    context= {'finta':finta,'name1':name1,'rcfab':rcfab,'trcfab':trcfab,'qrcfab':qrcfab}


    return render(request,'dash/region.html',context)







def distributors (request,dist):
    # sup=str(rep)
    # dam=sup.split(' ')
    # tam='+'.join(dam)

    # sup1=str(asm)
    # dam1=sup1.split(' ')
    # tam1='+'.join(dam1)



    eba=requests.get(f'{api}/semi/?REGION=&CHANNEL={dist}&TEAM={kapu}&ASM=&REP=')
    eba1= eba.json()
    eba2=pd.DataFrame(eba1)
    eba3=eba2['id'].tolist()

    name1=dist

    pr=requests.get(f"{api}/collection/?Date__gte={lqgap}&Date__lte={qgap}")

    ## year to date
    pr1=pr.json()
    pr2=pd.DataFrame(pr1)
    pr22=pr2[pr2['semi'].isin(eba3)]
    pr3=(pr22['Amount'].sum())/1000000

    ## Last year
    # prm=requests.get(f'{api}/Prevcollection/?Date__gte={lqdap2}&Date__lte={qdap2}')
    # prm1=prm.json()
    # prm2=pd.DataFrame(prm1)
    # prm22=prm2[prm2['semi'].isin(eba3)]
    # prm3=(prm22['Amount'].sum())/1000000


    opy=requests.get(f"{api}/Budget/?procode=&TEAM={kapu}")
    opy1=opy.json()
    opy2=pd.DataFrame(opy1)
    opy3=opy2[opy2['semi'].isin(eba3)]
    opy4=((opy3['buz'].sum())/1000000)*rwe2
    opy5=((opy3['buz'].sum())/1000000)


    ## ytd collection growth
    # colgrowth=(pr3-prm3)/prm3

    ## ytd budget achiived
    sca=(pr3/opy4)*100


    ##MTD for collection
    ##current
    # mpr=pr22[(pr22['Date']<=Rqdap)&(pr22['Date']>=Rqdap2)]
    # mpr2=(mpr['Amount'].sum())/1000000

    mpr=requests.get(f"{api}/collection/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    mpra=mpr.json()
    mprb=pd.DataFrame(mpra)
    mprc=mprb[mprb['semi'].isin(eba3)]
    mpr2=(mprc['Amount'].sum())/1000000

    ## previous


    ## mtd budget achiived for collection
    mcba=(mpr2/opy5)*100






    spr=requests.get(f'{api}/current/?TEAM=&Date__gte={lqgap}&Date__lte={qgap}&pro=')

    ## year to date
    spr1=spr.json()
    spr2=pd.DataFrame(spr1)
    spr22=spr2[spr2['semi'].isin(eba3)]
    spr3=(spr22['Money'].sum())/1000000

    ## Last year
    sprm=requests.get(f'{api}/Previous/?Date__gte={lqdap2}&Date__lte={qdap2}')
    sprm1=sprm.json()
    sprm2=pd.DataFrame(sprm1)
    sprm22=sprm2[sprm2['semi'].isin(eba3)]
    sprm3=(sprm22['Money'].sum())/1000000


    ##MTD forsales
    ##current for sales
    smpr=requests.get(f"{api}/current/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    smpra=smpr.json()
    smprb=pd.DataFrame(smpra)
    smprc=smprb[smprb['semi'].isin(eba3)]
    smpr2=(smprc['Money'].sum())/1000000

    ## previous


    ## mtd budget achiived for sales
    smcba=(smpr2/opy5)*100





    ## sales growth
    salgrowth=((spr3-sprm3)/sprm3)*100

    ## budget achiived
    sba=(spr3/opy4)*100



    finta={'curcol':pr3,'ytdbudget':opy4,'mtdbudget':opy5,'mcol':mpr2,'mcba':mcba,'sca':sca,'cursales':spr3,
        'lastsales':sprm3,'salgrowth':salgrowth,'sba':sba,'msal':smpr2,'msa':smcba}
    

    # ______________________________________________________________________


        ## ytd
    prod1=(spr22.groupby(['pro','product'])[['ctns']].sum()).reset_index()
    prod2=(sprm22.groupby(['pro'])[['ctns']].sum()).reset_index()
    prod3=(opy3.groupby(['procode'])[['value']].sum()).reset_index()


    ## merge the ytd records 
    prod4=pd.merge(prod1,prod2,on='pro',how='left')
    prod5=pd.merge(prod4,prod3,left_on='pro',right_on='procode',how='left')
    prod5['tgt']=prod5['value']*rwe2

    ## mtd
    rod1=(smprc.groupby(['pro','product'])[['ctns']].sum()).reset_index()


    prod6=pd.merge(prod5,rod1,on='pro',how='left')
    prod7=prod6.fillna(0)


    ##get the percentages
    prod7['ytdA']=(prod7['ctns_x']/prod7['tgt'])*100
    prod7['mtdA']=(prod7['ctns']/prod7['value'])*100
    prod7['gwt']=((prod7['ctns_x']-prod7['ctns_y'])/prod7['ctns_y'])*100

    rcbsr=prod7.to_json(orient='records')
    rcfab=[]
    rcfab=json.loads(rcbsr)
    rcfab



    #-------------------------TREND CHART------------------------------------------

    # lprm=requests.get(f'{api}/Prevcollection/?Date__gte=&Date__lte=')
    # lprm1=lprm.json()
    # lprm2=pd.DataFrame(lprm1)
    # lprm22=lprm2[lprm2['semi'].isin(eba3)]
    # lprm22['num']=pd.DatetimeIndex(lprm22['Date']).month
    # cur2=lprm22.groupby(['num'])['Amount'].sum()

    pr23=pr22.copy()
    pr23['num']=pd.DatetimeIndex(pr23['Date']).month
    cur1=pr23.groupby(['num'])[['Amount']].sum()


    ## merge all
    ped1=pd.merge(sunk,cur1,on='num',how='left')
    # ped2=pd.merge(ped1,cur2,on='num',how='left')

    ped3=ped1.fillna(0)

    ped3['budget']=opy5
    ped3['Amount_x1']=ped3['Amount']/1000000
    # ped3['Amount_y1']=ped3['Amount_y']/1000000


    trcbsr=ped3.to_json(orient='records')
    trcfab=[]
    trcfab=json.loads(trcbsr)
    trcfab


    #--------------------------------Last 7 days collection-------------------------------

    
    y=qgap
    y1=datetime.datetime.strptime(y,'%Y-%m-%d')
    y9=y1.strftime("%Y-%m-%d")
    y10=y1.strftime("%w")
    y11=y1.strftime('%A')
    cin=[y9]
    gol=[y10]
    zol=[y11]





    for i in range(1,20):
        y2=y1 - datetime.timedelta(days=1)
        y7=y2.strftime("%Y-%m-%d")
        y3=y2.strftime('%w')
        yin=y2.strftime('%A')
        if y3=='0' or y3=='6':
            pass
        else:
            cin.append(y7)
            gol.append(y3)
            zol.append(yin)
        if len(cin)>4:
            break
        y1=y2



    lad=pd.DataFrame({'dated':cin,'nos':gol,'day':zol})





    lad['dated']=lad['dated'].astype('datetime64[ns]')
    pr29=pr22.copy()
    pr29['Date']=pr29['Date'].astype('datetime64[ns]')
    pr2023=pr29.groupby(['Date'])[['Amount']].sum()

    parf=pd.merge(lad,pr2023,left_on='dated',right_on='Date',how='left')
    parf2=parf.fillna(0)
    parf2['dated']=parf2['dated'].astype(str)

    qrcbsr=parf2.to_json(orient='records')
    qrcfab=[]
    qrcfab=json.loads(qrcbsr)
    qrcfab

    

    context= {'finta':finta,'name1':name1,'rcfab':rcfab,'trcfab':trcfab,'qrcfab':qrcfab}


    return render(request,'dash/dist.html',context)



def team (request):
    # sup=str(rep)
    # dam=sup.split(' ')
    # tam='+'.join(dam)

    # sup1=str(asm)
    # dam1=sup1.split(' ')
    # tam1='+'.join(dam1)



    eba=requests.get(f'{api}/semi/?REGION=&CHANNEL=&TEAM={kapu}&ASM=&REP=')
    eba1= eba.json()
    eba2=pd.DataFrame(eba1)
    eba3=eba2['id'].tolist()

    name1=team

    pr=requests.get(f"{api}/collection/?Date__gte={lqgap}&Date__lte={qgap}")

    ## year to date
    pr1=pr.json()
    pr2=pd.DataFrame(pr1)
    pr22=pr2[pr2['semi'].isin(eba3)]
    pr3=(pr22['Amount'].sum())/1000000

    ## Last year
    # prm=requests.get(f'{api}/Prevcollection/?Date__gte={lqdap2}&Date__lte={qdap2}')
    # prm1=prm.json()
    # prm2=pd.DataFrame(prm1)
    # prm22=prm2[prm2['semi'].isin(eba3)]
    # prm3=(prm22['Amount'].sum())/1000000


    opy=requests.get(f"{api}/Budget/?procode=&TEAM={kapu}")
    opy1=opy.json()
    opy2=pd.DataFrame(opy1)
    opy3=opy2[opy2['semi'].isin(eba3)]
    opy4=((opy3['buz'].sum())/1000000)*rwe2
    opy5=((opy3['buz'].sum())/1000000)


    ## ytd collection growth
    # colgrowth=(pr3-prm3)/prm3

    ## ytd budget achiived
    sca=(pr3/opy4)*100


    ##MTD for collection
    ##current
    # mpr=pr22[(pr22['Date']<=Rqdap)&(pr22['Date']>=Rqdap2)]
    # mpr2=(mpr['Amount'].sum())/1000000

    mpr=requests.get(f"{api}/collection/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    mpra=mpr.json()
    mprb=pd.DataFrame(mpra)
    mprc=mprb[mprb['semi'].isin(eba3)]
    mpr2= (mprc['Amount'].sum())/1000000

    ## previous


    ## mtd budget achiived for collection
    mcba=(mpr2/opy5)*100






    spr=requests.get(f'{api}/current/?TEAM=&Date__gte={lqgap}&Date__lte={qgap}&pro=')

    ## year to date
    spr1=spr.json()
    spr2=pd.DataFrame(spr1)
    spr22=spr2[spr2['semi'].isin(eba3)]
    spr3=(spr22['Money'].sum())/1000000

    ## Last year
    sprm=requests.get(f'{api}/Previous/?Date__gte={lqdap2}&Date__lte={qdap2}')
    sprm1=sprm.json()
    sprm2=pd.DataFrame(sprm1)
    sprm22=sprm2[sprm2['semi'].isin(eba3)]
    sprm3=(sprm22['Money'].sum())/1000000


    ##MTD forsales
    ##current for sales
    smpr=requests.get(f"{api}/current/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    smpra=smpr.json()
    smprb=pd.DataFrame(smpra)
    smprc=smprb[smprb['semi'].isin(eba3)]
    smpr2=(smprc['Money'].sum())/1000000

    ## previous


    ## mtd budget achiived for sales
    smcba=(smpr2/opy5)*100





    ## sales growth
    salgrowth=((spr3-sprm3)/sprm3)*100

    ## budget achiived
    sba=(spr3/opy4)*100



    finta={'curcol':pr3,'ytdbudget':opy4,'mtdbudget':opy5,'mcol':mpr2,'mcba':mcba,'sca':sca,'cursales':spr3,
        'lastsales':sprm3,'salgrowth':salgrowth,'sba':sba,'msal':smpr2,'msa':smcba}
    

    # ______________________________________________________________________


        ## ytd
    prod1=(spr22.groupby(['pro','product'])[['ctns']].sum()).reset_index()
    prod2=(sprm22.groupby(['pro'])[['ctns']].sum()).reset_index()
    prod3=(opy3.groupby(['procode'])[['value']].sum()).reset_index()


    ## merge the ytd records 
    prod4=pd.merge(prod1,prod2,on='pro',how='left')
    prod5=pd.merge(prod4,prod3,left_on='pro',right_on='procode',how='left')
    prod5['tgt']=prod5['value']*rwe2

    ## mtd
    rod1=(smprc.groupby(['pro','product'])[['ctns']].sum()).reset_index()


    prod6=pd.merge(prod5,rod1,on='pro',how='left')
    prod7=prod6.fillna(0)


    ##get the percentages
    prod7['ytdA']=(prod7['ctns_x']/prod7['tgt'])*100
    prod7['mtdA']=(prod7['ctns']/prod7['value'])*100
    prod7['gwt']=((prod7['ctns_x']-prod7['ctns_y'])/prod7['ctns_y'])*100

    rcbsr=prod7.to_json(orient='records')
    rcfab=[]
    rcfab=json.loads(rcbsr)
    rcfab



    #-------------------------TREND CHART------------------------------------------

    # lprm=requests.get(f'{api}/Prevcollection/?Date__gte=&Date__lte=')
    # lprm1=lprm.json()
    # lprm2=pd.DataFrame(lprm1)
    # lprm22=lprm2[lprm2['semi'].isin(eba3)]
    # lprm22['num']=pd.DatetimeIndex(lprm22['Date']).month
    # cur2=lprm22.groupby(['num'])['Amount'].sum()

    pr23=pr22.copy()
    pr23['num']=pd.DatetimeIndex(pr23['Date']).month
    cur1=pr23.groupby(['num'])[['Amount']].sum()


    ## merge all
    ped1=pd.merge(sunk,cur1,on='num',how='left')
    # ped2=pd.merge(ped1,cur2,on='num',how='left')

    ped3=ped1.fillna(0)

    ped3['budget']=opy5
    ped3['Amount_x1']=ped3['Amount']/1000000
    # ped3['Amount_y1']=ped3['Amount_y']/1000000


    trcbsr=ped3.to_json(orient='records')
    trcfab=[]
    trcfab=json.loads(trcbsr)
    trcfab


    #--------------------------------Last 7 days collection-------------------------------

    
    y=qgap
    y1=datetime.datetime.strptime(y,'%Y-%m-%d')
    y9=y1.strftime("%Y-%m-%d")
    y10=y1.strftime("%w")
    y11=y1.strftime('%A')
    cin=[y9]
    gol=[y10]
    zol=[y11]





    for i in range(1,20):
        y2=y1 - datetime.timedelta(days=1)
        y7=y2.strftime("%Y-%m-%d")
        y3=y2.strftime('%w')
        yin=y2.strftime('%A')
        if y3=='0' or y3=='6':
            pass
        else:
            cin.append(y7)
            gol.append(y3)
            zol.append(yin)
        if len(cin)>4:
            break
        y1=y2



    lad=pd.DataFrame({'dated':cin,'nos':gol,'day':zol})





    lad['dated']=lad['dated'].astype('datetime64[ns]')
    pr29=pr22.copy()
    pr29['Date']=pr29['Date'].astype('datetime64[ns]')
    pr2023=pr29.groupby(['Date'])[['Amount']].sum()

    parf=pd.merge(lad,pr2023,left_on='dated',right_on='Date',how='left')
    parf2=parf.fillna(0)
    parf2['dated']=parf2['dated'].astype(str)

    qrcbsr=parf2.to_json(orient='records')
    qrcfab=[]
    qrcfab=json.loads(qrcbsr)
    qrcfab

    

    context= {'finta':finta,'name1':name1,'rcfab':rcfab,'trcfab':trcfab,'qrcfab':qrcfab}


    return render(request,'dash/team.html',context)


def marketing (request):
    # sup=str(rep)
    # dam=sup.split(' ')
    # tam='+'.join(dam)

    # sup1=str(asm)
    # dam1=sup1.split(' ')
    # tam1='+'.join(dam1)



    eba=requests.get(f'{api}/semi/?REGION=&CHANNEL=&TEAM={kapu}&ASM=&REP=&SEGMENT=Marketing')
    eba1= eba.json()
    eba2=pd.DataFrame(eba1)
    eba3=eba2['id'].tolist()

    name1=team

    pr=requests.get(f"{api}/collection/?Date__gte={lqgap}&Date__lte={qgap}")

    ## year to date
    pr1=pr.json()
    pr2=pd.DataFrame(pr1)
    pr22=pr2[pr2['semi'].isin(eba3)]
    pr3=(pr22['Amount'].sum())/1000000

    # ## Last year
    # prm=requests.get(f'{api}/Prevcollection/?Date__gte={lqdap2}&Date__lte={qdap2}')
    # prm1=prm.json()
    # prm2=pd.DataFrame(prm1)
    # prm22=prm2[prm2['semi'].isin(eba3)]
    # prm3=(prm22['Amount'].sum())/1000000


    opy=requests.get(f"{api}/Budget/?procode=&TEAM={kapu}")
    opy1=opy.json()
    opy2=pd.DataFrame(opy1)
    opy3=opy2[opy2['semi'].isin(eba3)]
    opy4=((opy3['buz'].sum())/1000000)*rwe2
    opy5=((opy3['buz'].sum())/1000000)


    ## ytd collection growth
    # colgrowth=(pr3-prm3)/prm3

    ## ytd budget achiived
    sca=(pr3/opy4)*100


    ##MTD for collection
    ##current
    # mpr=pr22[(pr22['Date']<=Rqdap)&(pr22['Date']>=Rqdap2)]
    # mpr2=(mpr['Amount'].sum())/1000000
    mpr=requests.get(f"{api}/collection/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    mpra=mpr.json()
    mprb=pd.DataFrame(mpra)
    mprc=mprb[mprb['semi'].isin(eba3)]
    mpr2=(mprc['Amount'].sum())/1000000

    ## previous


    ## mtd budget achiived for collection
    mcba=(mpr2/opy5)*100






    spr=requests.get(f'{api}/current/?TEAM=&Date__gte={lqgap}&Date__lte={qgap}&pro=')

    ## year to date
    spr1=spr.json()
    spr2=pd.DataFrame(spr1)
    spr22=spr2[spr2['semi'].isin(eba3)]
    spr3=(spr22['Money'].sum())/1000000

    ## Last year
    sprm=requests.get(f'{api}/Previous/?Date__gte={lqdap2}&Date__lte={qdap2}')
    sprm1=sprm.json()
    sprm2=pd.DataFrame(sprm1)
    sprm22=sprm2[sprm2['semi'].isin(eba3)]
    sprm3=(sprm22['Money'].sum())/1000000


    ##MTD forsales
    ##current for sales
    smpr=requests.get(f"{api}/current/?Date__gte={Rqdap2}&Date__lte={Rqdap}")
    smpra=smpr.json()
    smprb=pd.DataFrame(smpra)
    smprc=smprb[smprb['semi'].isin(eba3)]
    smpr2=(smprc['Money'].sum())/1000000

    ## previous


    ## mtd budget achiived for sales
    smcba=(smpr2/opy5)*100





    ## sales growth
    salgrowth=((spr3-sprm3)/sprm3)*100

    ## budget achiived
    sba=(spr3/opy4)*100



    finta={'curcol':pr3,'ytdbudget':opy4,'mtdbudget':opy5,'mcol':mpr2,'mcba':mcba,'sca':sca,'cursales':spr3,
        'lastsales':sprm3,'salgrowth':salgrowth,'sba':sba,'msal':smpr2,'msa':smcba}
    

    # ______________________________________________________________________


        ## ytd
    prod1=(spr22.groupby(['pro','product'])[['ctns']].sum()).reset_index()
    prod2=(sprm22.groupby(['pro'])[['ctns']].sum()).reset_index()
    prod3=(opy3.groupby(['procode'])[['value']].sum()).reset_index()


    ## merge the ytd records 
    prod4=pd.merge(prod1,prod2,on='pro',how='left')
    prod5=pd.merge(prod4,prod3,left_on='pro',right_on='procode',how='left')
    prod5['tgt']=prod5['value']*rwe2

    ## mtd
    rod1=(smprc.groupby(['pro','product'])[['ctns']].sum()).reset_index()


    prod6=pd.merge(prod5,rod1,on='pro',how='left')
    prod7=prod6.fillna(0)


    ##get the percentages
    prod7['ytdA']=(prod7['ctns_x']/prod7['tgt'])*100
    prod7['mtdA']=(prod7['ctns']/prod7['value'])*100
    prod7['gwt']=((prod7['ctns_x']-prod7['ctns_y'])/prod7['ctns_y'])*100

    rcbsr=prod7.to_json(orient='records')
    rcfab=[]
    rcfab=json.loads(rcbsr)
    rcfab



    #-------------------------TREND CHART------------------------------------------

    # lprm=requests.get(f'{api}/Prevcollection/?Date__gte=&Date__lte=')
    # lprm1=lprm.json()
    # lprm2=pd.DataFrame(lprm1)
    # lprm22=lprm2[lprm2['semi'].isin(eba3)]
    # lprm22['num']=pd.DatetimeIndex(lprm22['Date']).month
    # cur2=lprm22.groupby(['num'])['Amount'].sum()

    pr23=pr22.copy()
    pr23['num']=pd.DatetimeIndex(pr23['Date']).month
    cur1=pr23.groupby(['num'])[['Amount']].sum()


    ## merge all
    ped1=pd.merge(sunk,cur1,on='num',how='left')
    # ped2=pd.merge(ped1,cur2,on='num',how='left')

    ped3=ped1.fillna(0)

    ped3['budget']=opy5
    ped3['Amount_x1']=ped3['Amount']/1000000
    # ped3['Amount_y1']=ped3['Amount_y']/1000000


    trcbsr=ped3.to_json(orient='records')
    trcfab=[]
    trcfab=json.loads(trcbsr)
    trcfab


    #--------------------------------Last 7 days collection-------------------------------

    
    y=qgap
    y1=datetime.datetime.strptime(y,'%Y-%m-%d')
    y9=y1.strftime("%Y-%m-%d")
    y10=y1.strftime("%w")
    y11=y1.strftime('%A')
    cin=[y9]
    gol=[y10]
    zol=[y11]





    for i in range(1,20):
        y2=y1 - datetime.timedelta(days=1)
        y7=y2.strftime("%Y-%m-%d")
        y3=y2.strftime('%w')
        yin=y2.strftime('%A')
        if y3=='0' or y3=='6':
            pass
        else:
            cin.append(y7)
            gol.append(y3)
            zol.append(yin)
        if len(cin)>4:
            break
        y1=y2



    lad=pd.DataFrame({'dated':cin,'nos':gol,'day':zol})





    lad['dated']=lad['dated'].astype('datetime64[ns]')
    pr29=pr22.copy()
    pr29['Date']=pr29['Date'].astype('datetime64[ns]')
    pr2023=pr29.groupby(['Date'])[['Amount']].sum()

    parf=pd.merge(lad,pr2023,left_on='dated',right_on='Date',how='left')
    parf2=parf.fillna(0)
    parf2['dated']=parf2['dated'].astype(str)

    qrcbsr=parf2.to_json(orient='records')
    qrcfab=[]
    qrcfab=json.loads(qrcbsr)
    qrcfab

    

    context= {'finta':finta,'name1':name1,'rcfab':rcfab,'trcfab':trcfab,'qrcfab':qrcfab}


    return render(request,'dash/marketing.html',context)


def home(request):
    return render(request,'dash/home.html')









