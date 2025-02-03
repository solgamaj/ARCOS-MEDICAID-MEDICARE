import pandas as pd
import re


search_values = ["oxycod"]

cols=["Prscrbr_Geo_Lvl", "Prscrbr_Geo_Cd", "Prscrbr_Geo_Desc", "Brnd_Name", "Gnrc_Name", "Tot_Prscrbrs", "Tot_Clms", "Tot_30day_Fills", "Tot_Drug_Cst", "Tot_Benes", "GE65_Sprsn_Flag", "GE65_Tot_Clms", "GE65_Tot_30day_Fills", "GE65_Tot_Drug_Cst", "GE65_Bene_Sprsn_Flag", "GE65_Tot_Benes", "LIS_Bene_Cst_Shr", "NonLIS_Bene_Cst_Shr", "Opioid_Drug_Flag", "Opioid_LA_Drug_Flag", "Antbtc_Drug_Flag", "Antpsyct_Drug_Flag"]
pd.DataFrame(columns=["Year","Prscrbr_Geo_Cd","Prscrbr_Geo_Desc","Tot_Prscrbrs", "Tot_Clms", "Tot_30day_Fills", "Tot_Drug_Cst", "Tot_Benes", "GE65_Tot_Clms", "GE65_Tot_30day_Fills", "GE65_Tot_Drug_Cst", "GE65_Tot_Benes", "LIS_Bene_Cst_Shr", "NonLIS_Bene_Cst_Shr"]).to_csv("totalY.csv",index=False)
for i in range(2013,2023): #all downloaded files were named *year*.csv
    year=str(i)
    #manually setting column types because pandas got mad and started eating memory
    raw = pd.read_csv("Raw Data/"+year+".csv", dtype={0:"string",1:"string",2:"string",3:"string",4:"string",5:float,6:float,7:float,8:float,9:float,10:"string",11:float,12:float,13:float,14:"string",15:float,16:float,17:float,18:"string",19:"string",20:"string",21:"string"})
    raw.columns=cols

    #pull out wanted rows and save a file with everything left
    raw.insert(0,"Year",[int(year)]*len(raw))
    oxy = raw.loc[raw["Gnrc_Name"].str.contains('|'.join(search_values),flags=re.IGNORECASE)]
    raw.drop(raw.loc[raw["Gnrc_Name"].str.contains('|'.join(search_values),flags=re.IGNORECASE)].index, inplace=True)
    raw.to_csv("Check/"+year+".csv")

    oxy.to_csv("Extracted/"+year+".csv")
    with open("totalY.csv",'a') as tY:
        oxy.groupby(["Year","Prscrbr_Geo_Cd","Prscrbr_Geo_Desc"])[["Tot_Prscrbrs", "Tot_Clms", "Tot_30day_Fills", "Tot_Drug_Cst", "Tot_Benes", "GE65_Tot_Clms", "GE65_Tot_30day_Fills", "GE65_Tot_Drug_Cst", "GE65_Tot_Benes", "LIS_Bene_Cst_Shr", "NonLIS_Bene_Cst_Shr"]].apply(lambda x : x.sum()).to_csv(tY, header=False)

