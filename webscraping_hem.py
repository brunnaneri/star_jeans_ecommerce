#Imports library

import re
import os
import json
import numpy      as np
import pandas     as pd
import sqlite3
import logging
import requests

from datetime import datetime
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

# =================== Data Collection =========================

def data_collection(url,hearders):

    requests_data = requests.get(url, headers=headers)
    page = requests_data.text

    soup = BeautifulSoup(page, 'html.parser')
    total_products = soup.find('h2',class_="load-more-heading")['data-total']
    items_pages = soup.find('h2',class_="load-more-heading")['data-items-shown']
    pagination = round(int(total_products)/int(items_pages))
    total_show = int(items_pages)*pagination

    # Criando listas vazias e iterador
    i = int(items_pages)
    aux1 = []
    aux2 = []
    aux3 = []

    # iteração que permite coletar todos os produtos de todas as páginas
    while i <= int(total_show):

        if i <= (i * pagination):

            url02 = 'https://www2.hm.com/en_us/men/products/jeans.html?offset=0&page-size=' + str(i)

            # API Request
            requests_2 = requests.get(url02, headers=headers)
            page_2 = requests_2.text
            soup_2 = BeautifulSoup(page_2, 'html.parser')

            number_products = soup_2.find_all('li', class_='product-item')

            products_id = [soup_2.find_all('article', class_='hm-product-item')[j]['data-articlecode'] for j in
                           range(len(number_products))]
            aux1 = aux1 + products_id

            name_products = [soup_2.find_all('div', class_="image-container")[k].find('a')['title'] for k in
                             range(len(number_products))]
            aux2 = aux2 + name_products

            price_products = [
                list(filter(None, soup_2.find_all('strong', class_="item-price")[u].find('span').get_text().split('$'))) for
                u in range(len(number_products))]
            aux3 = aux3 + price_products

            i = int(items_pages) + i
            print(url02)

        else:
            print('fim')

    product_id=pd.DataFrame(aux1)
    product_name=pd.DataFrame(aux2)
    product_price=pd.DataFrame(aux3)

    df1 = pd.concat([product_id,product_name],axis=1)
    data = pd.concat([df1,product_price],axis=1)
    data.columns = ['product_id','product_name','product_price']

    data = data.drop_duplicates(subset='product_id', keep="first")
    data = data.reset_index(drop=True)

    return data

#==================== Data Collection by product =========================

def collection_by_product(data):
    #Get Collor

    aux_color = pd.DataFrame()

    for i in range(len(data)):
        #API requests
        url = 'https://www2.hm.com/en_us/productpage.'+data.loc[i,'product_id']+'.html'
        logger.debug('Color: %s',url)
        requests_color = requests.get(url,headers = headers)
        page_color = requests_color.text
        soup_color = BeautifulSoup(page_color,'html.parser')
        list_item = soup_color.find('div',class_='mini-slider').find_all('li',class_="list-item")
        color_item = [list_item[i].find('a')['data-color'] for i in range(len(list_item))]
        code_item = [list_item[i].find('a')['data-articlecode'] for i in range(len(list_item))]
        color_name = pd.DataFrame(color_item)
        code_item = pd.DataFrame(code_item)
        item_id = pd.concat([color_name,code_item],axis=1)
        aux_color = pd.concat([item_id,aux_color],axis=0)

    aux_color.columns = ['name_color','product_id']
    df_color = aux_color.copy()
    df_color = df_color.drop_duplicates()
    df_color = df_color.reset_index(drop=True)

    # Get Price and Name product

    aux_price = []
    aux_name = []
    product_id = []

    for j in range(len(df_color)):
        url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[j, 'product_id'] + '.html'
        logger.debug('Price: %s', url)
        requests_price = requests.get(url, headers=headers)
        page_price = requests_price.text
        soup_price = BeautifulSoup(page_price, 'html.parser')
        price_product = [
            soup_price.find('section', class_='name-price').find('div', class_='primary-row product-item-price').get_text()]
        name_product = [
            soup_price.find('section', class_='name-price').find('h1', class_='primary product-item-headline').string]
        aux_price = price_product + aux_price
        aux_name = name_product + aux_name
        product_id = [df_color.loc[j, 'product_id']] + product_id

    aux_price = pd.DataFrame(aux_price)
    aux_name = pd.DataFrame(aux_name)
    product_id = pd.DataFrame(product_id)
    aux_price.columns = ['product_price']
    aux_name.columns = ['product_name']
    product_id.columns = ['product_id']
    df_price = pd.concat([aux_price,product_id],axis=1)
    df_info = pd.concat([df_price,aux_name],axis=1)

    # Get Composition

    aux_composition = pd.DataFrame()

    for k in range(len(df_color)):
        url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[k, 'product_id'] + '.html'
        logger.debug('Composition: %s', url)
        requests_composition = requests.get(url, headers=headers)
        page_composition = requests_composition.text
        soup_composition = BeautifulSoup(page_composition, 'html.parser')
        composition_list = soup_composition.find_all('div', class_="details parbase")[0].find_all('script')[1].string
        composition_list = composition_list.replace('\r', '').replace('\t', '').replace('\n', '').replace('\\"', '').replace("\\'", "")
        p = re.search('\[.*(s*{.*}s*).*\]', composition_list).group(0)
        # needs to be " " to the json.loads work
        p = p.replace("'", '"')
        p = p.replace('title', '"title"')
        p = p.replace('values', '"values"')
        json_data = json.loads(p)
        composition = pd.DataFrame.from_dict(json_data)
        df_composition = (composition).T
        df_composition.columns = df_composition.iloc[:1].iloc[0, :].tolist()
        df_composition = df_composition.reset_index(drop=True)
        df_composition = df_composition.drop(0)
        for u in range(len(df_composition.columns)):
            df_composition.iloc[0, u] = df_composition.iloc[0, u][0]

        aux_composition = pd.concat([aux_composition, df_composition], axis=0)

    aux_composition = aux_composition.reset_index(drop=True)
    aux_composition['scrapy_datetime'] = datetime.now().strftime( '%Y-%m-%d %H:%M:%S' )
    df_composition=aux_composition.copy()
    df_composition = df_composition.drop(columns=['More sustainable materials'])
    #df_composition.columns = ['fit','composition','product_id','size','scrapy_datetime']
    df_composition.rename(columns={'Art. No.': 'product_id'}, inplace=True)
    df_composition.columns = df_composition.columns.str.lower()

    #Final join

    df_details = df_info.merge(df_color,how='inner', on='product_id' )
    df = df_details.merge(df_composition, how='inner',on='product_id')
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    return df

#==================== Data Cleaned =========================

def data_cleaned(data):
    #Name color
    data['name_color'] = data['name_color'].str.lower()
    data['name_color'] = data['name_color'].str.replace(" ","_")

    #Fit
    data['fit'] = data['fit'].str.lower()
    data['fit'] = data['fit'].str.replace(" ","_")

    #Name Product
    data['product_name'] = data['product_name'].str.replace('\n','')
    data['product_name'] = data['product_name'].str.replace('\t','')
    data['product_name'] = data['product_name'].str.lower()
    data['product_name'] = data['product_name'].str.replace("  ","")
    data['product_name'] = data['product_name'].str.replace(" ","_")

    #Size
    data['size_model'] = data['size'].apply(lambda x: re.search('(\d+cm)',x).group(0) if  pd.notnull( x ) else x)
    data['product_size'] = data['size'].apply(lambda x: re.search('(size)(.*)',x).group(2) if  pd.notnull( x ) else x)

    data['product_price']=data['product_price'].str.replace('\n','')
    data['product_price']=data['product_price'].str.replace('\r','')
    data['product_price']=data['product_price'].str.replace('$','')

    df_ref = data['composition'].str.split(',', expand=True)
    df_ref.loc[:,0] = df_ref.loc[:,0].str.replace('Shell:','')

    df1 = pd.DataFrame(index=np.arange(len(data)),columns=['cotton','spandex','polyester','elastomultiester','modal'])

    #Cotton
    df1['cotton'] = df_ref[0]

    #Spandex
    df_spandex_1 = df_ref.loc[df_ref[1].str.contains('Spandex',na=True),1]
    df_spandex_1.name = 'spandex'

    df_spandex_2 = df_ref.loc[df_ref[2].str.contains('Spandex',na=True),2]
    df_spandex_2.name = 'spandex'

    df_spandex_3 = df_ref.loc[df_ref[3].str.contains('Spandex',na=True),3]
    df_spandex_3.name = 'spandex'

    df_spandex_c1 = df_spandex_1.combine_first(df_spandex_2)
    df_spandex = df_spandex_c1.combine_first(df_spandex_3)

    df1 = pd.concat( [df1, df_spandex], axis=1 )
    df1 = df1.iloc[:, ~df1.columns.duplicated( keep='last') ]

    #Elastomultiester
    df_elastomultiester = df_ref.loc[df_ref[1].str.contains('Elastomultiester',na=True),1]
    df_elastomultiester.name='elastomultiester'
    df1 = pd.concat([df1,df_elastomultiester],axis=1)
    df1 = df1.iloc[:,~df1.columns.duplicated(keep='last')]

    #Modal
    df_modal = df_ref.loc[df_ref[2].str.contains('Modal',na=True),2]
    df_modal.name = 'modal'
    df1 = pd.concat([df1,df_modal],axis=1)
    df1 = df1.iloc[:,~df1.columns.duplicated(keep='last')]

    #Polyester
    df_polyester = df_ref.loc[df_ref[1].str.contains('Polyester',na=True),1]
    df_polyester.name = 'polyester'
    df1 = pd.concat([df1,df_polyester],axis=1)
    df1 = df1.iloc[:,~df1.columns.duplicated(keep='last')]

    #None to NaN
    df1['cotton'].fillna(value=np.nan, inplace=True)
    df1['spandex'].fillna(value=np.nan, inplace=True)
    df1['elastomultiester'].fillna(value=np.nan, inplace=True)
    df1['modal'].fillna(value=np.nan, inplace=True)
    df1['polyester'].fillna(value=np.nan, inplace=True)

    data = pd.concat([data,df1],axis=1)

    data['cotton']=data['cotton'].fillna('Cotton 0%')
    data['polyester']=data['polyester'].fillna('Polyester 0%')
    data['spandex']=data['spandex'].fillna('Spandex 0%')
    data['elastomultiester']=data['elastomultiester'].fillna('Elastomultiester 0%')
    data['modal']=data['modal'].fillna('Modal 0%')

    data['cotton'] = data['cotton'].apply(lambda x: int(re.search('(\d+)',x).group(1))/100 if pd.notnull(x) else x)
    data['polyester'] = data['polyester'].apply(lambda x: int(re.search('(\d+)',x).group(1))/100 if pd.notnull(x) else x)
    data['spandex'] = data['spandex'].apply(lambda x: int(re.search('(\d+)',x).group(1))/100 if pd.notnull(x) else x)
    data['elastomultiester'] = data['elastomultiester'].apply(lambda x: int(re.search('(\d+)',x).group(1))/100 if pd.notnull(x) else x)
    data['modal'] = data['modal'].apply(lambda x: int(re.search('(\d+)',x).group(1))/100 if pd.notnull(x) else x)

    data = data.drop_duplicates(keep='first')
    data = data.drop(columns=['composition','size'])

    return data
#=================== to sqlite 3 =====================

def data_insert(data):
    df_table = data[['product_price',
                     'product_id',
                     'product_name',
                     'name_color',
                     'fit',
                     'size_model',
                     'product_size',
                     'cotton',
                     'spandex',
                     'elastomultiester',
                     'modal',
                     'polyester',
                     'scrapy_datetime'
                     ]]

    #CRIANDO O BD - COM SQLALCHEMY
    db_hem = create_engine('sqlite:///db_hem.sqlite',echo=False)

    #Connect db
    conn = db_hem.connect()

    ##Inserindo os dados na tabela criada
    #
    df_table.to_sql('hem_products',con=conn,if_exists='append',index=False)

    return None

if __name__== "__main__":
    #loggings
    path = 'C:/Users/Brunna/star_jeans_ecommerce/webscraping_hem/'

    if not os.path.exists(path + 'Logs'):
        os.makedirs(path+'Logs')

    logging.basicConfig(
        filename = path + 'Logs/webscraping_hem.log',
        level = logging.DEBUG,
        format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger('webscraping_hem')

    #constantes
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    #Data Collect
    data_collect = data_collection(url,headers)
    logger.info('data collect done')

    #Data Collect by product
    data_product = collection_by_product(data_collect)
    logger.info('data collection_by_product done')

    #Data cleaned
    data_product_cleaned = data_cleaned(data_product)
    logger.info('data product cleaned done')

    #Data save in bd
    data_insert(data_product_cleaned)
    logger.info('data insertion done')
























