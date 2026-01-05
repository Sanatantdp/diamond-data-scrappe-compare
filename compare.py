# import pandas as pd

# tdp_df=pd.read_csv('tdp.csv',low_memory=False, on_bad_lines='skip')

# tdp_df = tdp_df.rename(columns={
#     "carat_weight": "t_d_carat",
#     "shape": "t_d_shape",
#     "color": "t_d_color",
#     "clarity": "t_d_clarity"
# })

# novita_df=pd.read_csv('novita_diamonds.csv',low_memory=False, on_bad_lines='skip')

# tdp_df["certificate_number"] = tdp_df["certificate_number"].astype(str)

# novita_df["certificate_number"] = novita_df["certificate_number"].astype(str)

# merged = pd.merge(
#     tdp_df,
#     novita_df,
#     on="certificate_number",
#     how="inner"
# )
# final = merged[["certificate_number",'t_d_carat','t_d_shape','t_d_color','t_d_clarity', "final_price_aud", "price"]]
# final = final.rename(columns={
#     "certificate_number": "Certificate No",
#     "t_d_carat": "Carat",
#     't_d_shape':'Shape',
#     't_d_color':'Color',
#     't_d_clarity':'Clarity',
#     "final_price_aud": "TDP price aud",
#     "price": "Novita Price AUD"
# })
# # price_ranges = [
# #    ]
# # def get_markup(price):
# #     for r in price_ranges:
# #         if r["min"] <= price <= r["max"]:
# #             return r["markup"]
# def highlight_negative(v):
#     return ['background-color: red' if x < 0 else '' for x in v]


# # final["TDP Markup Price USD"] = final["TDP Price USD"].apply(get_markup)

# # final["TDP Price USD"] = round((final["TDP Price USD"] ))

# final["Novita Price AUD"] = round(final["Novita Price AUD"].astype(int)*1.10)
# final['TDP price aud'] = round(final["TDP price aud"]*(0.70))

# final['compare(Nivota - TDP) AUD'] = (
#     final["Novita Price AUD"] - final["TDP price aud"]
# ).round(2)

# styled = final.style.apply(
#     highlight_negative,
#     subset=['compare(Nivota - TDP) AUD']
# )

# styled.to_excel("compare.xlsx", index=False)

# # final.to_csv("compare.csv", index=False)
# # final['compare(Nivota - TDP) AUD']=['background-color: red' if v < 0 else '' for v in final['compare(Nivota - TDP) AUD']]
# # final.style.apply(subset='compare(Nivota - TDP) AUD')
# # print(final)
# # # print(final.info())
# # # print(final.describe())

# # final.to_csv('compare.csv')


############################################## FOR COMPARE CULLEN DIAMONDS #####################################3


# import pandas as pd

# tdp_df=pd.read_csv('tdp.csv',low_memory=False, on_bad_lines='skip')

# tdp_df = tdp_df.rename(columns={
#     "carat_weight": "t_d_carat",
#     "shape": "t_d_shape",
#     "color": "t_d_color",
#     "clarity": "t_d_clarity"
# })

# cullen_df=pd.read_csv('cullen_diamond.csv',low_memory=False, on_bad_lines='skip')

# tdp_df["certificate_number"] = tdp_df["certificate_number"].astype(str)

# cullen_df["certificate_number"] = cullen_df["certificate_number"].astype(str)

# merged = pd.merge(
#     tdp_df,
#     cullen_df,
#     on="certificate_number",
#     how="inner"
# )
# final = merged[["certificate_number",'t_d_carat','t_d_shape','t_d_color','t_d_clarity', "final_price_aud", "price"]]
# final = final.rename(columns={
#     "certificate_number": "Certificate No",
#     "t_d_carat": "Carat",
#     't_d_shape':'Shape',
#     't_d_color':'Color',
#     't_d_clarity':'Clarity',
#     "final_price_aud": "TDP Price AUD",
#     "price": "Cullen Price AUD"
# })

# def highlight_negative(v):
#     return ['background-color: red' if x < 0 else '' for x in v]

# final["TDP Price AUD"] = round((final["TDP Price AUD"] ))
# final['TDP price Dis 30% AUD'] = round(final["TDP Price AUD"]*(0.70))
# final["Cullen Price AUD"] = round(final["Cullen Price AUD"].astype(int))
# final['compare(Cullen - TDP) AUD'] = (
#     final["Cullen Price AUD"] - final["TDP price Dis 30%/ AUD"]
# ).round(2)
# print(final)
# print(final.info())
# print(final.describe())

# styled = final.style.apply(
#     highlight_negative,
#     subset=['compare(Cullen - TDP) AUD']
# )

# styled.to_excel("compare_cullen.xlsx", index=False)

# final.to_csv("compare.csv", index=False)
# final['compare(Nivota - TDP) AUD']=['background-color: red' if v < 0 else '' for v in final['compare(Nivota - TDP) AUD']]
# final.style.apply(subset='compare(Nivota - TDP) AUD')

# final.to_csv('compare.csv')

#####################################################Brilliance diamond compare ##################################################

import pandas as pd
import os

def compare_with_tdp(
    tdp_csv,
    vendor_csv,
    vendor_name,
    vendow_certi_col,
    vendor_price_col,
    tdp_discount=0.70  
):
    base_dir=os.getcwd()
    tdp_file_path=os.path.join(base_dir,tdp_csv)
    vendor_file_path=os.path.join(base_dir,vendor_csv)
    
    if not os.path.exists(tdp_file_path):
        print(f'{tdp_csv} file is not exist')
    if not os.path.exists(vendor_file_path):
        print(f'{vendor_csv} file is not exist')
    
    tdp = pd.read_csv(tdp_csv, low_memory=False, on_bad_lines="skip")
    vendor = pd.read_csv(vendor_csv, low_memory=False, on_bad_lines="skip")

    tdp = tdp.rename(columns={
        "carat_weight": "t_d_carat",
        "shape": "t_d_shape",
        "color": "t_d_color",
        "clarity": "t_d_clarity",
        "certificate_number": "certificate_number",
        "final_price_usd": "PC Price USD"
    })

    tdp["certificate_number"] = tdp["certificate_number"].astype(str)
    vendor["certificate_number"] = vendor[vendow_certi_col].astype(str)

    merged = pd.merge(tdp, vendor, on="certificate_number", how="inner")

    final = merged[
        [
            "certificate_number",
            "t_d_carat",
            "t_d_shape",
            "t_d_color",
            "t_d_clarity",
             'url',
            "PC Price USD",
            vendor_price_col,
           
        ]
    ].copy()

    final = final.rename(columns={
        "certificate_number": "Certificate No",
        "t_d_carat": "Carat",
        "t_d_shape": "Shape",
        "t_d_color": "Color",
        "t_d_clarity": "Clarity",
        f"{vendor_name} Diamond URL":"url",
        vendor_price_col: f"{vendor_name} Price USD"
        
    })

    final["PC Price USD"] = final["PC Price USD"].astype(float).round(0)
    final[f"PC -30% USD"] = (final["PC Price USD"] * tdp_discount).round(0)
    final[f"{vendor_name} Price USD"] = final[f"{vendor_name} Price USD"].str.replace(',','').astype(int)

    final[f"Compare ({vendor_name} - PC)USD"] = (
        final[f"{vendor_name} Price USD"] - final[f"PC -30% USD"]
    ).round(2)
    # final 
    
    def highlight_loss(v):
        return ["background-color:red" if x < 0 else "" for x in v]

    styled = final.style.apply(
        highlight_loss,
        subset=[f"Compare ({vendor_name} - PC)USD"]
    )
    styled.to_excel(f"PC_{vendor_name}_compare.xlsx", index=False)

    print(f"{vendor_name} comparison created")
    print(final.describe())
    print(final.head())
    return final


compare_with_tdp(tdp_csv='tdp_usd.csv',vendor_csv='Brilliance_diamonds.csv',vendor_name='Brilliance',vendor_price_col='price',vendow_certi_col='reportNumber')
