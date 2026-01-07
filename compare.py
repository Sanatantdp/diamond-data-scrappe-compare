
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
            #  'url',
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
        # f"{vendor_name} Diamond URL":"url",
        vendor_price_col: f"{vendor_name} Price USD"
        
    })

    final["PC Price USD"] = final["PC Price USD"].astype(float).round(0)
    final[f"PC -30% USD"] = (final["PC Price USD"] * tdp_discount).round(0)
    # final[f"{vendor_name} Price USD"] = final[f"{vendor_name} Price USD"].str.replace(',','').astype(int)
    final[f"{vendor_name} Price USD"] = (
    final[f"{vendor_name} Price USD"]
    .astype(str)
    .str.replace(",", "")
    .apply(lambda x: int(float(x)) if x.replace(".","",1).isdigit() else 0)
)

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


compare_with_tdp(tdp_csv='tdp_usd.csv',vendor_csv='loosegrowndiamond.csv',vendor_name='Loosegrown',vendor_price_col='price',vendow_certi_col='sku')
