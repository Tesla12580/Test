# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 09:57:20 2018

@author: fsheng
"""
#This is  to show  that the file is created in Oct 3rd.

import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt
import statsmodels.api as sm
import time
import datetime
import logging
import pdblp
import solver
import os
import pandas_gbq
os.chdir(r'P:\Drop Box\KMV_all_0815')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
#import pdblp
logging.debug('step1 get data for investment grade and recommendation')
#step0 get the ticker we want

#read the dataframe
df_ticker_all=pd.read_excel('ticker_all.xlsx')

ticker_hy1=list(df_ticker_all.loc[df_ticker_all.High_Yield==1,'Ticker'])
ticker_ig1=list(df_ticker_all.loc[df_ticker_all.Investment_Grade==1,'Ticker'])
ticker_port1=list(df_ticker_all.loc[df_ticker_all.Portfolio==1,'Ticker'])
ticker_rcmd1=list(df_ticker_all.loc[df_ticker_all.Recommendation==1,'Ticker'])
ticker_all1=list(df_ticker_all.Ticker)
ticker_bond_blank1=list(df_ticker_all.loc[df_ticker_all.Bond_ISIN.isnull(),'Ticker'])
#%%
#step1 get bond for company tickers which donnot have a bond connected with it in a table provided

#deal with the data got form bloomberg
def step1_dealdata(df):
    df_copy=df.copy()
    df_copy.loc[:,'Identifier']=df_copy.value.copy()
    for i in df_copy.position:
        df_copy.loc[df_copy.position==i,'Identifier']=[list(df_copy.loc[df_copy.position==i,'Identifier'])[0]]*2
    df_copy=df_copy.loc[df_copy.value!=df_copy.Identifier]
    #df_copy.set_index('Date',inplace=True)
    df_copy.rename(columns={'value':'Value'},inplace=True)
    df_copy.drop(['name','position'],axis=1,inplace=True)
    df_copy.index=range(len(df_copy))
    df_copy=df_copy[['ticker','Identifier','Value']]
    return df_copy

#for every equity get the bond with maturity closet to five year maturity
def get_bond(name):
    with pdblp.bopen(port=8194,timeout=50000) as bb:
        bond_chain=step1_dealdata(bb.bulkref(name,"BOND_CHAIN"))
        date=[]
        for i in bond_chain.loc[:,"Value"]:
            #i=bond_chain.loc[:,"Value"][0]
            temp_list=i.split(" ")[2].split("/")
            temp_date=datetime.date(int('20'+temp_list[2]),int(temp_list[0]),int(temp_list[1]))
            date.append(temp_date)
        today=datetime.date.today()
        date_diff=[abs((i-today).days-365*5) for i in date]
        bond=bond_chain.loc[date_diff.index(min(date_diff))]
        bond.loc['ISIN']=bb.ref(bond.Identifier,'ID_ISIN').loc[0,'value']
        return bond

#actual step to get the bond
step1_summary_bond=pd.DataFrame(columns=['ticker','Identifier','Value','ISIN'])
step1_gebond_excpt=[]
for i in (ticker_bond_blank1):
    try:
        temp_bond=get_bond(i)
        step1_summary_bond=step1_summary_bond.append(temp_bond)
        print('step1 get bond for investment grade and recommendation, all is:',len(ticker_bond_blank1),'now running:',list(ticker_bond_blank1).index(i))
    except:
        step1_gebond_excpt.append(i)
        print('step1 get bond for investment grade and recommendation',sys.exc_info())
        continue
step1_gebond_excpt=pd.DataFrame({"Ticker":step1_gebond_excpt})
step1_gebond_excpt.to_csv('step1_gebond_excpt.csv')
step1_summary_bond.index=range(len(step1_summary_bond))
step1_summary_bond.rename(columns={'ticker':'Ticker'},inplace=True)
#step1_summary_bond.to_csv("step1_summary_bond.csv")

for i in range(len(df_ticker_all)):
    try:
        if df_ticker_all.loc[i,'Ticker'] in ticker_bond_blank1:
            df_ticker_all.loc[i,'Bond_ISIN']=step1_summary_bond.loc[step1_summary_bond.Ticker==df_ticker_all.loc[i,'Ticker'],'ISIN'].iloc[0]
    except:
        print(sys.exc_info())

#bb = pdblp.BCon(debug=True)
#bb.start()

for i in range(len(df_ticker_all)):
    with pdblp.bopen(port=8194,timeout=50000) as bb:
        try:
            df_ticker_all.loc[i,'Spread']=bb.ref('/isin/'+df_ticker_all.loc[i,'Bond_ISIN'],'YAS_OAS_SPRD').value.iloc[0]
            df_ticker_all.loc[i,'Yield']=bb.ref('/isin/'+df_ticker_all.loc[i,'Bond_ISIN'],'YAS_BOND_YLD').value.iloc[0]
            df_ticker_all.loc[i,'Rating']=bb.ref('/isin/'+df_ticker_all.loc[i,'Bond_ISIN'],'BB_COMPOSITE').value.iloc[0]
            print('step1 get,spread,yield,bloomberg rating for bond, all is:',\
                  len(df_ticker_all),'now running:',i,df_ticker_all.loc[i,'Ticker'])
        except:
            print('step1 get,spread,yield,bloomberg rating for bond, all is:',\
                  len(df_ticker_all),'now running:',i,df_ticker_all.loc[i,'Ticker'])
            print(sys.exc_info())



#%%
#step2 get data from bloomberg
#first set of columns we want

ask1=['NAME','PARSEKYABLE_DES','ID_ISIN','ID_EXCH_SYMBOL','ID_CUSIP','ID_BB','COUNTRY','CNTRY_OF_RISK',\
                  'GICS_SECTOR_NAME','GICS_INDUSTRY_NAME','GICS_INDUSTRY_GROUP_NAME','GICS_SUB_INDUSTRY_NAME','DDIS_CURRENCY','MARKET_STATUS']
#because GICS_SECTOR_NAME and MARKET STATUS cannot be got from the bdh function, so adjust the value

ask2=['CRNCY_ADJ_PX_LAST','VOLATILITY_360D','CRNCY_ADJ_MKT_CAP','EBITDA','EBIT','IS_INT_EXPENSE','CAPITAL_EXPEND']
#con.ref('AAPL US Equity',ask2,ovrds=[('EQY_FUND_CRNCY','USD'),('DDIS_CURRENCY','USD')])
#second sets of columns we want
ask3=['CRNCY_ADJ_CURR_EV','CASH_AND_MARKETABLE_SECURITIES','SHORT_AND_LONG_TERM_DEBT','EBITDA_ADJUSTED','CURRENT_EV_TO_EBITA','TOTAL_DEBT_TO_EV']

#third sets of columns we want
ask4=['CAST_AMT_OUTSTDG_TOTAL_DEBT','CAST_AMT_OUTSTDG_TOTAL_SECD_DEBT','CAST_AMT_OUTSTDG_TOT_UNSEC_DEBT','CAST_AMT_OUTSTDG_DIP_LOANS',\
   'CAST_AMT_OUTSTDG_1ST_LIEN_LOANS','CAST_AMT_OUTSTDG_1ST_LIEN_BONDS','CAST_AMT_OUTSTDG_SR_SECD_BONDS',\
   'CAST_AMT_OUTSTDG_2ND_LIEN_LOANS','CAST_AMT_OUTSTDG_2ND_LIEN_BONDS','CAST_AMT_OUTSTDG_3RD_LIEN_LOANS',\
   'CAST_AMT_OUTSTDG_MEZZ_LOANS','CAST_AMT_OUTSTDG_UNSECURED_LOANS','CAST_AMT_OUTSTDG_SR_UNSEC_BONDS',\
   'CAST_AMT_OUTSTDG_GOVT_GUAR_BONDS','CAST_TOT_SUB_DEBT_AMT_OUTSTDG','DDIS_CURRENCY']

#tags change
#"CAST_AMT_OUTSTDG_SR_SECD_BONDS" to CAST_SECURED_BONDS_AMT_OUTSTDG
#
#No values for these tickers
#"CAST_AMT_OUTSTDG_SR_SECD_BONDS"
#"CAST_AMT_OUTSTDG_MEZZ_LOANS"
#"CAST_AMT_OUTSTDG_GOVT_GUAR_BONDS"
#"DDIS_CURRENCY"

ask42=['CAST_AMT_OUTSTDG_TOTAL_DEBT','CAST_AMT_OUTSTDG_TOTAL_SECD_DEBT','CAST_AMT_OUTSTDG_TOT_UNSEC_DEBT','CAST_AMT_OUTSTDG_DIP_LOANS',\
   'CAST_AMT_OUTSTDG_1ST_LIEN_LOANS','CAST_AMT_OUTSTDG_1ST_LIEN_BONDS','CAST_SECURED_BONDS_AMT_OUTSTDG',\
   'CAST_AMT_OUTSTDG_2ND_LIEN_LOANS','CAST_AMT_OUTSTDG_2ND_LIEN_BONDS','CAST_AMT_OUTSTDG_3RD_LIEN_LOANS',\
   'CAST_AMT_OUTSTDG_UNSECURED_LOANS','CAST_AMT_OUTSTDG_SR_UNSEC_BONDS',\
   'CAST_TOT_SUB_DEBT_AMT_OUTSTDG']


step2_summary=pd.DataFrame(columns=['Ticker']+ask1+ask2+ask3+ask42)
step2_getdata_excpt=[]
#con = pdblp.BCon(debug=True)
#con.start()
#k=con.ref('BHARTI IN Equity',ask1+ask2+ask3+ask42,ovrds=[('EQY_FUND_CRNCY','USD'),('DDIS_CURRENCY','USD')])
for i in ticker_all1:
    with pdblp.bopen(port=8194,timeout=50000) as bb:
        try:


            #locals()[i4+'_orgn']=(bb.bdh(i,['CUR_MKT_CAP','CAST_AMT_OUTSTDG_TOTAL_DEBT'],'20130601','20180611'))[::-1]
            #in the default mode, usually the CUR_MKT_CAP is in the currency of the country the company belongs to
            #CAST_AMT_OUTSTDG_TOTAL_DEBT is usually in USD, for example, for BHARTI, CUR_MKT_CAP is in INR, CAST_AMT_OUTSTDG is in USD


            temp=bb.ref(i,ask1+ask2+ask3+ask42,ovrds=[('EQY_FUND_CRNCY','USD'),('DDIS_CURRENCY','USD')])#currency is USA
            #use ref to get data, usually these data cannot use bdh function to get the historical data

            temp=temp.T
            temp.columns=temp.loc['field']
            temp=temp.loc['value']
            temp2=pd.Series({'Ticker':i})
            temp2=temp2.append(temp)

            step2_summary=step2_summary.append(temp2,ignore_index=True)

            print('step2 get fundamental data',ticker_all1.index(i))


        except:
            print('step2 get fundamental data',i)
            print(sys.exc_info())
            step2_getdata_excpt.append(i)
            continue

#%%
#step3 merton model calculation
try:
    #locals()[i]=locals()[i][0:20]
    t=0;r=0.028;T=5;#what to use as risk free interest rate?
    #still to assue that the maturity date is 5 years
    K=[float(j) for j in step2_summary.SHORT_AND_LONG_TERM_DEBT];
    #markt value is in million, CAST_AMT_OUTSTDG_TOTAL_DEBT is in thousand
    sig_E=[float(j)/100 for j in step2_summary.VOLATILITY_360D]
    #summary_rcmd_cal.CUR_MKT_CAP=[float(j)*1000 for j in summary_rcmd_cal.CUR_MKT_CAP] ;
    E_t=[float(j) for j in step2_summary.CRNCY_ADJ_MKT_CAP];

    try:
        step2_summary['MDL_A_t'],step2_summary['MDL_sig_A'],step2_summary['MDL_D_t'],\
        step2_summary['MDL_Sprd'],step2_summary['MDL_Deft_Prob'],step2_summary['MDL_Recvr']\
        =solver.calc_merton_list(t,T,r,K,sig_E,E_t)
        #summary_rcmd_cal['MDL_Deft_ProbPerct']=['{0:.2%}'.format(j) for j in summary_rcmd_cal['MDL_Deft_Prob']]

    except:

        print('step2 merton model calculation',sys.exc_info())
except:
    print(sys.exc_info())

step3_summary=pd.merge(df_ticker_all,step2_summary,on='Ticker')
pandas_gbq.to_gbq(step3_summary,
                  destination_table='RealTime_reports.Credit_Model',
                  project_id='constellation-capital',
                  if_exists='replace',
                  private_key='constellation-capital.json')
# ['EBITDA','EBIT','IS_INT_EXPENSE','CAPITAL_EXPEND','TOT_DEBT_TO_EBITDA',\
#  'DEBT_TO_MKT_CAP','TOT_DEBT_TO_TOT_CAP','DEBT_TO_MKT_CAP',\
#  'TR_12_MO_EBITDA_TR_12_M_INT_EXPN','EBITDA_TO_INTEREST_EXPN',\
#  'EBIT_TO_INT_EXP','5Y_MID_CDS_SPREAD','RSK_BB_IMPLIED_CDS_SPREAD','BB_1YR_DEFAULT_PROB','BB_5Y_DEFAULT_PROB']

#credit swap settings
