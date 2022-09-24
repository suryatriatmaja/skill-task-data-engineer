# Note untuk Skill Test eFishery Soal-1
1. Dags di jalankan setiap jam 7 pagi dengan schedule :
```
0 7 * * *
```
2. Berikut ada schema DWH Alternatif yang telah dibuat :
```
https://dbdiagram.io/d/632ef6c57b3d2034ffa708ba
```
- Schema DWH diubah agar lebih efektif dalam pengolahan data nantinya. Pada table DWH fact_order_accumulating field order_date_id, invoice_date_id, payment_date_id dihilangkan karena dirasa tidak dibutuhkan. Nama schema DWH = efishery_data_warehouse.
- Berikut adalah result success dags yang sudah dijalankan di airflow version 2
![image](https://user-images.githubusercontent.com/42970673/192100380-5c0e81ee-399b-4611-b214-71db420d5d69.png)

  

# Panduan Menjalankan Program Python Skill Test eFishery Soal-2

## Persiapan Module
1. Install python3/python3 lates
2. Install pandas menggunakan command berikut :
```
pip install pandas
```
3. Json
4. Re

Untuk module json dan re sudah terinstall default pada python.

## Run Program
Berikut adalah syntax untuk menjalankan program :
```
python3 soal-2.py
```
