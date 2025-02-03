import pandas as pd
import re
import urllib.request 

start = 2000
end = 2024

#download raw data
#for i in range(start,end):
    #urllib.request.urlretrieve("https://download.medicaid.gov/data/StateDrugUtilizationData" + str(i) + ".csv", "Raw Data/" + str(i) + ".csv")

#fx to reformat ndc numbers from FDA directory
def fix_ndc(ndc):
    n=ndc.split("-")
    fixed=n[0].zfill(5) + n[1].zfill(4) + n[2].zfill(2)
    return fixed

ref = pd.read_csv("NDC.csv")#ndc numbers downloaded from directory
ndcs = [fix_ndc(n) for n in ref["NDC Package Code"].tolist()]

with open("NDC_Codes.csv",'a') as codes:
        nums=pd.DataFrame(ndcs,columns={"NDC Code"})
        nums.insert(1,column="Name",value=pd.Series((ref[["Proprietary Name","Non Proprietary Name"]].astype('str').agg('|'.join,axis=1)).tolist()))
        nums.to_csv(codes)

search_values = [re.escape(n) for n in ref["Proprietary Name"].unique().tolist()] + ["oxyco","oxyce","oxayd","oxect","oxydo","oxyfa","oxyir","naloc","percoc","percoda","percolo","prolat","endoco","endoce","magnac","roxico","roxilo","roxipri","roxybo","tylox","combuno","xtamp","dazido"]

cols=["Utilization Type", "State","NDC", "Labeler Code", "Product Code", "Package Size","Year", "Quarter", "Suppression Used", "Product Name", "Units Reimbursed", "Number of Prescriptions", "Total Amount Reimbursed", "Medicaid Amount Reimbursed", "Non Medicaid Amount Reimbursed"]
pd.DataFrame(columns=["Year","Quarter","State","Units Reimbursed","Number of Prescriptions","Total Amount Reimbursed","Medicaid Amount Reimbursed","Non Medicaid Amount Reimbursed"]).to_csv("totalQ.csv",index=False)
pd.DataFrame(columns=["Year","State","Units Reimbursed","Number of Prescriptions","Total Amount Reimbursed","Medicaid Amount Reimbursed","Non Medicaid Amount Reimbursed"]).to_csv("totalY.csv",index=False)
for i in range(start,end): #all downloaded files were named *year*.csv
    year=str(i)
    #manually setting column types due to memory issue likely from pandas trying to parse types
    raw = pd.read_csv("Raw Data/"+year+".csv", dtype={0:"string",1:"string",2:"string",3:"string",4:"string",5:"string",6:"string",7:"string",8:"string",9:"string",10:float,11:float,12:float,13:float,14:float})
    raw.columns=["Utilization Type", "State","NDC", "Labeler Code", "Product Code", "Package Size","Year", "Quarter", "Suppression Used", "Product Name", "Units Reimbursed", "Number of Prescriptions", "Total Amount Reimbursed", "Medicaid Amount Reimbursed", "Non Medicaid Amount Reimbursed"]

    #pull out wanted rows and save a file with everything left
    oxy = raw.loc[raw["Product Name"].str.contains('|'.join(search_values),flags=re.IGNORECASE) | raw["NDC"].astype(str).str.contains('|'.join(ndcs))]
    raw.drop(raw.loc[raw["Product Name"].str.contains('|'.join(search_values),flags=re.IGNORECASE) | raw["NDC"].astype(str).str.contains('|'.join(ndcs))].index, inplace=True)
    raw.to_csv("Check/"+year+".csv")

    #file with the NDC #s if you want to check
    with open("NDC_Codes.csv",'a') as codes:
        nums=pd.DataFrame(pd.Series(oxy["NDC"].tolist(),dtype='float64'),columns={"NDC Code"})
        nums.insert(1,column="Name",value=pd.Series(oxy["Product Name"].tolist(),dtype='str'))
        nums.to_csv(codes, header=False)

    oxy.to_csv("Extracted/"+year+".csv")
    with open("totalQ.csv",'a') as tQ:
        oxy.groupby(["Year","Quarter","State"])[["Units Reimbursed","Number of Prescriptions","Total Amount Reimbursed","Medicaid Amount Reimbursed","Non Medicaid Amount Reimbursed"]].apply(lambda x : x.sum()).to_csv(tQ, header=False)
    with open("totalY.csv",'a') as tY:
        oxy.groupby(["Year","State"])[["Units Reimbursed","Number of Prescriptions","Total Amount Reimbursed","Medicaid Amount Reimbursed","Non Medicaid Amount Reimbursed"]].apply(lambda x : x.sum()).to_csv(tY, header=False)

codes = pd.read_csv("NDC_Codes.csv")
codes.groupby(["NDC Code", "Name"]).size().reset_index().rename(columns={0:'count'}).to_csv("NDC_Codes_Agg.csv")

