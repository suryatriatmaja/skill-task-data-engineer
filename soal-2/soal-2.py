import json
import re
import pandas as pd

# Filter untuk cleansing data komoditas
def filter_cleansing_komoditas(y):
    if y != '':
        if y not in 'kepala,hitam,ikn,sotobayam,bakar,sate,food,merah,soto,usus,ayam,babat,kikil,pecel,nasi,uduk,dll,bebek,goreng':
            if y == 'sea':
                y = 'laut'
            if '-' in y:
                y = y.split('-')[0]
            if y == 'salem':
                y = 'salam'
            if y == 'lelw' or y == 'kele':
                y = 'lele'
            if y == 'kembug' or y == 'gembung':
                y = 'kembung'
            if y == 'parin':
                y = 'patin'
            if y == 'mujir' or y == 'majaer' or y == 'muajir' or y == 'mujaer' or y == 'jaer':
                y = 'mujair'
            if y == 'krapu' or y == 'kerpu':
                y = 'kerapu'
            if y == 'man' or y == 'emas':
                y = 'mas'
            if y == 'nil' or y == 'nilem':
                y = 'nila'
            if y == 'tingkol' or y == 'tngkol':
                y = 'tongkol'
            if y == 'gurami':
                y = 'gurame'

            return y

#Regex data komoditas 
def regex_komoditas(data_raw_json):
    komoditas = data_raw_json['komoditas'].replace(' dan ',',').replace('ikan','')
    komoditas = re.sub('[^A-Za-z0-9 -]+', ',', komoditas)
    return komoditas

#Regex data berat
def regex_berat(berat):
    berat = re.sub('[^0-9,]+', ',', berat)
    return berat


if __name__ == "__main__":
    #Read data file json soal-2.json
    f = open("soal-2.json")
    #Load data from file to data json
    json_list = json.load(f)

    list_all_json = []
    #Proces parsing, mapping & cleansing data json
    for data_raw_json in json_list:
        komoditas = regex_komoditas(data_raw_json)
        split_data = komoditas.split(',')
        list_kmdt = []
        #Process collect data komoditas & append data to list
        for x in split_data:
            for y in x.split(' '):
                if y !='':
                    list_kmdt.append(filter_cleansing_komoditas(y))
        for x in list_kmdt:
            if x == None:
                list_kmdt.remove(x)

        #Process collect data berat & append data to list
        berat = data_raw_json['berat']
        berat = regex_berat(berat)
        berat = berat.split(',')
        list_berat = []
        for x in berat:
            if x !='':
                list_berat.append(x)

        if len(list_kmdt) == len(list_berat):
            i = 0
            while i < len(list_kmdt):
                data = {"komoditas":list_kmdt[i],"berat":int(list_berat[i])}
                list_all_json.append(data)
                i +=1
        else:
            for x in list_kmdt:
                try:
                    data = {"komoditas":x,"berat":int(list_berat[0])}
                    list_all_json.append(data)
                except:
                    data = {"komoditas":x,"berat":0}
                    list_all_json.append(data)


    #Load list data json to dataframe pandas
    df = pd.DataFrame(list_all_json)
    #Menggunakan function groupby pandas untuk melakukan sum berat
    df = df.groupby('komoditas').sum()

    #Print result
    num_list = 1
    for x in df.itertuples():
        print(str(num_list)+". "+str(x[0])+": "+str(x [1])+"kg")
        num_list +=1