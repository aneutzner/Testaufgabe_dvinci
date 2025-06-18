import json
import pandas as pd

# Dateien laden
with open('kunden-info.json', 'r') as f:
    info = json.load(f)

with open('kunden-crm.json', 'r') as f:
    crm = json.load(f)

with open('dvinci-pricing.json', 'r') as f:
    pricing = json.load(f)

# DataFrames erstellen
df_info = pd.DataFrame(info)
df_crm = pd.DataFrame(crm)
df_pricing = pd.DataFrame(pricing)

 # die Göße der Tabelle 
print(df_info.shape)     
print(df_crm.shape)
print(df_pricing.shape)
# Inhalt der Tabellen 
print(df_info)
print(df_crm)
print(df_pricing)

# Merge df_info und df_crm über 'id'
df_merge = pd.merge(df_info, df_crm, on='id', how='outer')
print(df_merge)

print(df_pricing.columns)
print(df_pricing['price-by-companySite'])
print(df_pricing['price-for-modules'])

print(df_info['companySize'].max())
print(df_info['companySize'].min())

intervalle = [0, 100, 250, 500, 1000, 10000]
labels = ['0-100', '101-250', '251-500', '501-1000', '1001-10000']

#neue Spalte CompanySize Kategorie - CompanySize wird der Kategorie zugeordnet
df_merge['companySize_cat'] = pd.cut(df_merge['companySize'], bins=intervalle, labels=labels, right=True)

print(df_merge[['id','companySize', 'companySize_cat']].head(10))

df_pricing.index = df_pricing.index.astype(str)

# Merge: df_merge mit price by company Size aus df_pricing  
df_merge = df_merge.merge(
    df_pricing['price-by-companySite'].rename('price_by_companySize'),
    left_on='companySize_cat',
    right_index=True,
    how='left'
)

print(df_merge[['id', 'companySize','companySize_cat', 'price_by_companySize']].head(10))

print(df_merge[['id','companySize_cat', 'modules']].head(10))

#Module je Kunde je Zeile generieren (ursprünglich als Liste)
df_exploded = df_merge.explode('modules')
print(df_exploded[['id', 'companySize_cat', 'price_by_companySize', 'modules']].head(10))

module_prices = df_pricing['price-for-modules'].to_dict()
print(module_prices)

# Modulpreise zuordnen
df_exploded['price_by_module'] = df_exploded['modules'].map(module_prices)
print(df_exploded.columns)
print(df_exploded[['id', 'companySize', 'price_by_companySize','modules', 'price_by_module']].head(10))

# Fehlende Preise (bei NaN in modules) als 0 setzen
df_exploded['price_by_module'] = df_exploded['price_by_module'].fillna(0)
print(df_exploded[['id', 'companySize', 'modules', 'price_by_module']].head(10))

# je Kunde die Summe der Modulpreise berechnen
module_sum = df_exploded.groupby('id')['price_by_module'].sum().reset_index()

# Mit df_merge zusammenführen
df_result = df_merge.merge(module_sum, on='id', how='left')

print(df_result.isna().sum())
# Fehlende Werte mit NA kennzeichnen
df_result['price_by_companySize' ] = df_result['companySize'].fillna(0)
df_result[['url','modules','companySize_cat', 'companySize']] =df_result[['url', 'modules','companySize_cat', 'companySize']].fillna('NA')

# Gesamtsumme berechnen
df_result['total_price'] = df_result['price_by_companySize'] + df_result['price_by_module']

# Ergebnis anzeigen
print(df_result[['id', 'price_by_companySize', 'price_by_module', 'total_price']])

df_result.to_csv("kunden-rechnung.csv", index=False)