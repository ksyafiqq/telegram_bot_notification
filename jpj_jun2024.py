import pandas as pd
import numpy as np

##Read from FTP- Data Mei 2024

from ftplib import FTP
import pandas as pd
import numpy as np

# Define the FTP server details
server = "10.251.44.135"
username = "padu"
password = "padu1241"

# Create an FTP object and connect to the server
ftp = FTP(server)
ftp.login(username, password)

# Change to the "csvPADU" directory
file_list = ftp.nlst()
ftp.cwd("/oga/JPJ/MAY24")
# Get list of file names in the current directory
file_list = ftp.nlst()

# Print the file list
print("Files in 'oga ftp' directory:")
for filename in file_list:
    print(filename)

# Specify the files you want to download
files_to_download = ["VEH PERSENDIRIAN", "VEH SELAIN PERSENDIRIAN", "VEH EV", "VEH IA_OKU", "CDL", "PDL"]

#  Download the specified files
for filename in files_to_download:
    with open(filename, "wb") as local_file:
        ftp.retrbinary(f"RETR {filename}", local_file.write)
        print(f"Downloaded: {filename}")

persendirian = pd.read_csv("VEH PERSENDIRIAN", sep='\t', encoding='latin-1', dtype=str, on_bad_lines='skip')
perdagangan = pd.read_csv("VEH SELAIN PERSENDIRIAN",sep='\t', encoding='latin-1', dtype=str, on_bad_lines='skip')
oku = pd.read_csv("VEH IA_OKU", sep='\t', encoding='latin-1', dtype=str, on_bad_lines='skip')
elektrik = pd.read_csv("VEH EV", sep='\t', encoding='latin-1', dtype=str, on_bad_lines='skip')
cdl = pd.read_csv("CDL", sep='\t', encoding='latin-1', dtype=str, on_bad_lines='skip')
pdl = pd.read_csv("PDL", sep='\t', header=None, encoding='latin-1', dtype=str, on_bad_lines='skip')

persendirian["STATUS"]="PERSENDIRIAN"
perdagangan["STATUS"]="PERDAGANGAN"
oku["STATUS"]="OKU"
elektrik["STATUS"]="ELEKTRIK"

##Data PDL tiada Header##
cdl.columns
list_name = ["KATEGORI_PENGGUNA", "NO_KP_BARU", "NO_KP_LAMA", "NO_POLIS", "NO_TENTERA", "KELAS_A", "KELAS_A1", "KELAS_B", "KELAS_C", "KELAS_D", "KELAS_F", "KELAS_G", "KELAS_H", "KELAS_I", "TARIKH_TAMAT", "ALAMAT1", "ALAMAT2", "ALAMAT3", "POSKOD", "BANDAR", "NEGERI"]
new_column_names = {index: item for index, item in enumerate(list_name)}
pdl.columns = [new_column_names.get(i, col) for i, col in enumerate(pdl.columns)]

jpj_subset1= pd.concat([persendirian, perdagangan, oku, elektrik], ignore_index=True)
jpj_subset1.columns
jpj_subset1.dtypes

# Strip leading and trailing whitespace from all entries in the column

# Apply str.strip() to all object (string) columns in the DataFrame
jpj_subset1[jpj_subset1.select_dtypes(['object']).columns] = jpj_subset1.select_dtypes(['object']).apply(lambda x: x.str.strip())
cdl[cdl.select_dtypes(['object']).columns] = cdl.select_dtypes(['object']).apply(lambda x: x.str.strip())
pdl[pdl.select_dtypes(['object']).columns] = pdl.select_dtypes(['object']).apply(lambda x: x.str.strip())

# jpj_subset1['MODEL'] = jpj_subset1['MODEL'].str.strip()
# jpj_subset1['KOD_ASAL_KEND'] = jpj_subset1['KOD_ASAL_KEND'].str.strip()
# jpj_subset1['BUATAN'] = jpj_subset1['BUATAN'].str.strip()

del persendirian
del perdagangan
del oku
del elektrik

# jpn_2024 = pd.read_csv(r"C:\Users\wanaznie.fatihah\Desktop\WAN AZNIE\PADU\ID.csv",  dtype=str)
jpn_2024 = pd.read_csv(r"\\10.21.45.172\collab_sharing\CDC\ID\ID_30107310_nokp_complete.csv",  dtype=str)

jpn_2024.columns
jpn_2024[(jpn_2024['NO_KP'].isna())]
# jpn_2024 = jpn_2024.drop(columns=['individuId'])
# jpn_2024 = jpn_2024.drop(columns=['dosmId'])

# jpn_2024_ori=jpn_2024
# jpn_2024=jpn_2024[~(jpn_2024['NO_KP'].isna())]
# tiada_nokp=jpn_2024_ori[(jpn_2024_ori['NO_KP'].isna())]

jpj_subset2 = jpj_subset1.merge(jpn_2024[['DOSM_ID','NO_IR','NO_KP']],left_on='NO_KP_BARU',right_on='NO_KP',how='left')

jpj_subset2_jpn_true=jpj_subset2[~(jpj_subset2['NO_KP'].isna())]

##Read from FTP- DB JPJ old

from ftplib import FTP
import pandas as pd
import numpy as np

# Define the FTP server details
server = "10.251.44.135"
username = "padu"
password = "padu1241"

# Create an FTP object and connect to the server
ftp = FTP(server)
ftp.login(username, password)

# Change to the "csvPADU" directory
file_list = ftp.nlst()
ftp.cwd("/oga/COLLAB_JUN2024")
# Get list of file names in the current directory
file_list = ftp.nlst()

# Print the file list
print("Files in 'oga ftp' directory:")
for filename in file_list:
    print(filename)

# Specify the files you want to download
files_to_download = ["C09_KENDERAAN_FINAL.csv"]

#  Download the specified files
for filename in files_to_download:
    with open(filename, "wb") as local_file:
        ftp.retrbinary(f"RETR {filename}", local_file.write)
        print(f"Downloaded: {filename}")

kenderaan_old = pd.read_csv("C09_KENDERAAN_FINAL.csv",  encoding='latin-1', dtype=str)

jpj_subset2_jpn_true.columns
kenderaan_old.columns

jpj_subset2_jpn_true['KELAS_BADAN'].value_counts()

jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'NAMA': 'NAMA_PEMUNYA_BERDAFTAR'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'NO_DAFTAR_KEND': 'NO_PENDAFTARAN'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'MODEL': 'NAMA_MODEL'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'KUASA_ENJIN': 'KEUPAYAAN_ENJIN'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'TAHUN_BUATAN': 'TAHUN_DIBUAT'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'TAHUN_DAFTAR': 'TARIKH_PENDAFTARAN'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'KOD_ASAL_KEND': 'STATUS_IMPORT_BAHARU_TERPAKAI'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'TARIKH_TAMAT_LKM': 'TARIKH_LUPUT_LKM'})
jpj_subset2_jpn_true=jpj_subset2_jpn_true.rename(columns={'KELAS_BADAN': 'JENIS_BADAN'})

# Add new column with empty values
jpj_subset2_jpn_true['JENIS_KENDERAAN_EHAILING'] = pd.Series(dtype='object')

jpj_subset3 = jpj_subset2_jpn_true[['DOSM_ID','NO_PENDAFTARAN', 
       'BUATAN', 'NAMA_MODEL', 'KEUPAYAAN_ENJIN',
       'BAHAN_BAKAR', 'KELAS_KEGUNAAN', 'JENIS_BADAN', 'TAHUN_DIBUAT',
       'TARIKH_PENDAFTARAN', 'STATUS_IMPORT_BAHARU_TERPAKAI',
       'TARIKH_LUPUT_LKM', 'STATUS', 'JENIS_KENDERAAN_EHAILING']]

del jpj_subset1
del jpj_subset2
del jpj_subset2_jpn_true
    
# Drop the column 'KADAR_LESEN_KENDERAAN_BERMOTOR' and 'JPN_ID'
kenderaan_old.columns
# kenderaan_old = kenderaan_old.drop(columns=['KADAR_LESEN_KENDERAAN_BERMOTOR'])
# kenderaan_old = kenderaan_old.drop(columns=['JPN_ID'])

# Filter kenderaan_old to include only rows where NO_PENDAFTARAN is not in jpj_subset3
filtered_kenderaan_old = kenderaan_old[~kenderaan_old['NO_PENDAFTARAN'].isin(jpj_subset3['NO_PENDAFTARAN'])]

# Concatenate the filtered rows from kenderaan_old with all rows from jpj_subset3
jpj_subset4 = pd.concat([jpj_subset3, filtered_kenderaan_old])

# Reset index if needed
jpj_subset4.reset_index(drop=True, inplace=True)

# jpj_subset4['BUATAN'] = jpj_subset4['BUATAN'].str.strip()

#semak
jpj_subset4[(jpj_subset4['NO_PENDAFTARAN'] == "UITM2509")].T
jpj_subset4[(jpj_subset4['NO_PENDAFTARAN'] == "KDW5855")].T

##LESEN

cdl.columns
pdl.columns

cdl=cdl.rename(columns={'KATEGORI_PENGGUNA': 'KATEGORI'})
pdl=pdl.rename(columns={'KATEGORI_PENGGUNA': 'KATEGORI'})

cdl['KATEGORI'].value_counts()
pdl['KATEGORI'].value_counts()

cdl["STATUS"]="CDL"
pdl["STATUS"]="PDL"

# # Apply str.strip() to all object (string) columns in the cdl DataFrame
# cdl[cdl.select_dtypes(['object']).columns] = cdl.select_dtypes(['object']).apply(lambda x: x.str.strip())

# # Apply str.strip() to all object (string) columns in the pdl DataFrame
# pdl[pdl.select_dtypes(['object']).columns] = pdl.select_dtypes(['object']).apply(lambda x: x.str.strip())

lesen_new = pd.concat([cdl, pdl])

lesen_new.columns

lesen_new['KELAS_A'].value_counts()
lesen_new['KELAS_A1'].value_counts()
lesen_new['KELAS_B'].value_counts()
lesen_new['KELAS_C'].value_counts()
lesen_new['KELAS_D'].value_counts()
lesen_new['KELAS_F'].value_counts()
lesen_new['KELAS_G'].value_counts()
lesen_new['KELAS_H'].value_counts()
lesen_new['KELAS_I'].value_counts()

lesen_new[lesen_new['KATEGORI'].str.contains("Orang Awam Malaysia",na=False)]
lesen_new.columns
lesen_new = lesen_new.rename(columns={'TARIKH_TAMAT': 'TARIKH_LUPUT_LESEN'})

lesen_new['NO_KP'] = lesen_new['NO_KP_BARU'].fillna(lesen_new['NO_KP_LAMA'])

KELAS_A = lesen_new[['NO_KP','KELAS_A','TARIKH_LUPUT_LESEN']]
KELAS_A = KELAS_A.rename(columns={'KELAS_A': 'KELAS'})
KELAS_A1  = lesen_new[['NO_KP','KELAS_A1','TARIKH_LUPUT_LESEN']]
KELAS_A1 = KELAS_A1.rename(columns={'KELAS_A1': 'KELAS'})
KELAS_B  = lesen_new[['NO_KP','KELAS_B','TARIKH_LUPUT_LESEN']]
KELAS_B = KELAS_B.rename(columns={'KELAS_B': 'KELAS'})
KELAS_C  = lesen_new[['NO_KP','KELAS_C','TARIKH_LUPUT_LESEN']]
KELAS_C = KELAS_C.rename(columns={'KELAS_C': 'KELAS'})
KELAS_D  = lesen_new[['NO_KP','KELAS_D','TARIKH_LUPUT_LESEN']]
KELAS_D = KELAS_D.rename(columns={'KELAS_D': 'KELAS'})
KELAS_F  = lesen_new[['NO_KP','KELAS_F','TARIKH_LUPUT_LESEN']]
KELAS_F = KELAS_F.rename(columns={'KELAS_F': 'KELAS'})
KELAS_G  = lesen_new[['NO_KP','KELAS_G','TARIKH_LUPUT_LESEN']]
KELAS_G = KELAS_G.rename(columns={'KELAS_G': 'KELAS'})
KELAS_H = lesen_new[['NO_KP','KELAS_H','TARIKH_LUPUT_LESEN']]
KELAS_H = KELAS_H.rename(columns={'KELAS_H': 'KELAS'})
KELAS_I = lesen_new[['NO_KP','KELAS_I','TARIKH_LUPUT_LESEN']]
KELAS_I = KELAS_I.rename(columns={'KELAS_I': 'KELAS'})

dataframes_to_concat = [KELAS_A, KELAS_A1, KELAS_B, KELAS_C, KELAS_D, KELAS_F, KELAS_G, KELAS_H, KELAS_I]
LESEN_MEMANDU = pd.concat(dataframes_to_concat, ignore_index=True)
LESEN_MEMANDU['KELAS'].value_counts()
LESEN_MEMANDU = LESEN_MEMANDU[~LESEN_MEMANDU['KELAS'].str.contains('90|11|20|1', case=False, na=False)]
len(LESEN_MEMANDU)

LESEN_MEMANDU['KELAS'] = LESEN_MEMANDU['KELAS'].str.strip()
LESEN_MEMANDU2 = LESEN_MEMANDU.replace('', np.nan)

LESEN_MEMANDU2=LESEN_MEMANDU2[~LESEN_MEMANDU2['KELAS'].isna()]

LESEN_MEMANDU2[LESEN_MEMANDU2['NO_KP']=="921230025200"]

LESEN_MEMANDU_LAST = LESEN_MEMANDU2.groupby(['NO_KP', 'TARIKH_LUPUT_LESEN'])['KELAS'].apply(list).reset_index()
LESEN_MEMANDU_LAST_2 = LESEN_MEMANDU_LAST.drop_duplicates(subset='NO_KP')

lesen_new2=LESEN_MEMANDU_LAST_2
del LESEN_MEMANDU_LAST_2
del KELAS_A
del KELAS_A1
del KELAS_B
del KELAS_C
del KELAS_D
del KELAS_F
del KELAS_G 
del KELAS_H
del KELAS_I
del LESEN_MEMANDU
del LESEN_MEMANDU2

lesen_new2 = lesen_new2.rename(columns={'NO_KP': 'NO_KP_BARU'})
lesen_new3 = lesen_new2.merge(jpn_2024[['DOSM_ID','NO_IR','NO_KP']],left_on='NO_KP_BARU',right_on='NO_KP',how='left')

lesen_new3_jpn_true=lesen_new3[~(lesen_new3['NO_KP'].isna())]

lesen_new3_jpn_true['KELAS'].value_counts()

import re

# Define a function to remove square brackets and split into a list
def process_kelas(kelas):
    if isinstance(kelas, str):  # Check if it's a string
        return re.findall(r'\b\w+\b', kelas)
    else:  # If it's already a list, return it unchanged
        return kelas

# Apply the function to the KELAS column
lesen_new3_jpn_true['KELAS2'] = lesen_new3_jpn_true['KELAS'].apply(process_kelas)

# Convert lists to strings without square brackets
lesen_new3_jpn_true['KELAS2'] = lesen_new3_jpn_true['KELAS2'].apply(lambda x: ', '.join(x))

lesen_new3_jpn_true = lesen_new3_jpn_true.drop(columns=['KELAS'])

lesen_new3_jpn_true=lesen_new3_jpn_true.rename(columns={'KELAS2': 'KELAS_LESEN_MEMANDU'})
lesen_new3_jpn_true=lesen_new3_jpn_true.rename(columns={'TARIKH_LUPUT_LESEN': 'TARIKH_LUPUT_LESEN_MEMANDU'})

lesen_new4 = lesen_new3_jpn_true[['DOSM_ID', 'TARIKH_LUPUT_LESEN_MEMANDU','KELAS_LESEN_MEMANDU']]

##Read from FTP- DB LESEN JPJ old

from ftplib import FTP
import pandas as pd
import numpy as np

# Define the FTP server details
server = "10.251.44.135"
username = "padu"
password = "padu1241"

# Create an FTP object and connect to the server
ftp = FTP(server)
ftp.login(username, password)

# Change to the "csvPADU" directory
file_list = ftp.nlst()
ftp.cwd("/oga/COLLAB_JUN2024")
# Get list of file names in the current directory
file_list = ftp.nlst()

# Print the file list
print("Files in 'oga ftp' directory:")
for filename in file_list:
    print(filename)

# Specify the files you want to download
files_to_download = ["C09_KENDERAAN_MAIN_FINAL.csv"]

#  Download the specified files
for filename in files_to_download:
    with open(filename, "wb") as local_file:
        ftp.retrbinary(f"RETR {filename}", local_file.write)
        print(f"Downloaded: {filename}")

lesen_old = pd.read_csv("C09_KENDERAAN_MAIN_FINAL.csv",  encoding='latin-1', dtype=str)

# Drop the column 'BIL_KENDERAAN'
lesen_old = lesen_old.drop(columns=['BIL_KENDERAAN'])
lesen_old = lesen_old.drop(columns=['JPN_ID'])

jpn_2024.columns
jpn_2024[jpn_2024['NO_KP']=="921230025200"]

lesen_new4[lesen_new4['DOSM_ID']=="20299100212503"]
lesen_old[lesen_old['DOSM_ID']=="20299100212503"]

# Filter lesen_old to include only rows where DOSM_ID is not in lesen_new4
filtered_lesen_old = lesen_old[~lesen_old['DOSM_ID'].isin(lesen_new4['DOSM_ID'])]

# Concatenate the filtered rows from lesen_old with all rows from lesen_new4
lesen_new5 = pd.concat([lesen_new4, filtered_lesen_old])

lesen_new5[lesen_new5['DOSM_ID']=="20669117118510"]

# Reset index if needed
lesen_new5.reset_index(drop=True, inplace=True)

lesen_new5

# nokp_new=jpn_2024[~jpn_2024['DOSM_ID'].isin(lesen_new5['DOSM_ID'])]

# nokp_new.columns
# nokp_new = nokp_new.drop(columns=['NO_KP'])

# # Add new column with empty values
# nokp_new['TARIKH_LUPUT_LESEN_MEMANDU'] = pd.Series(dtype='object')
# nokp_new['KELAS_LESEN_MEMANDU'] = pd.Series(dtype='object')
# nokp_new['BIL_KENDERAAN'] = pd.Series(dtype='object')
# # Change the dtype of 'BIL_KENDERAAN' to float64
# nokp_new['BIL_KENDERAAN'] = nokp_new['BIL_KENDERAAN'].astype('float64')
# nokp_new = nokp_new.drop(columns=['NO_KP'])

jpj_subset4.columns
lesen_new5ori=lesen_new5

##semak satu dosmid tidak wujud di jpn
lesen_new5=lesen_new5[lesen_new5['DOSM_ID'].isin(jpn_2024['DOSM_ID'])]
lesen_new5[lesen_new5['DOSM_ID']=="80850203508552"]
jpj_subset4[jpj_subset4['DOSM_ID']=="80850203508552"]

value_counts = jpj_subset4['DOSM_ID'].value_counts().reset_index()
value_counts.columns = ['DOSM_ID', 'Count']
value_counts=value_counts.rename(columns={'DOSM_ID': 'DOSM_ID_x'})
kenderaan_main = lesen_new5.merge(value_counts[['DOSM_ID_x','Count']],left_on='DOSM_ID',right_on='DOSM_ID_x',how='left')

kenderaan_main.columns
kenderaan_main=kenderaan_main.rename(columns={'Count': 'BIL_KENDERAAN'})

# Handle NaNs before conversion if necessary
kenderaan_main['BIL_KENDERAAN'] = kenderaan_main['BIL_KENDERAAN'].fillna(0).astype(int)

kenderaan_main = kenderaan_main.drop(columns=['DOSM_ID_x'])

kenderaan_main.columns
# nokp_new.columns

# kenderaan_main2= pd.concat([kenderaan_main, nokp_new], ignore_index=True)

# kenderaan_main2['BIL_KENDERAAN'] = kenderaan_main2['BIL_KENDERAAN'].fillna(0).astype(int)

kenderaan_main_final = kenderaan_main[['DOSM_ID', 'BIL_KENDERAAN','TARIKH_LUPUT_LESEN_MEMANDU','KELAS_LESEN_MEMANDU']]

kenderaan_detail_final = jpj_subset4

#SEMAK
jpn_2024[jpn_2024['DOSM_ID']=="20669117118510"]
kenderaan_main_final[kenderaan_main_final['DOSM_ID']=="20669117118510"]
kenderaan_detail_final[kenderaan_detail_final['DOSM_ID']=="20669117118510"]

kenderaan_main_final.columns
kenderaan_detail_final.columns

kenderaan_main_final['DOSM_ID'].value_counts()

#save to csv
kenderaan_main_final.to_csv(r'\\10.21.45.172\collab_sharing\KENDERAAN\kenderaan_main_20062024.csv', index=False)
kenderaan_detail_final.to_csv(r'\\10.21.45.172\collab_sharing\KENDERAAN\kenderaan_detail_20062024.csv', index=False)

kenderaan_main_final['DOSM_ID'].value_counts()

# kenderaan_detail_final = pd.read_csv(r"C:\Users\User PADU\OneDrive\PADU_WANAZNIE\kenderaan_detail.csv", nrows=5, dtype=str)