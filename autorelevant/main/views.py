# https://serpapi.com/yandex/yandex-ru-geo-codes.json
# https://xmlstock.com/geotargets-google.csv


import json
import csv
import os
import pandas as pd
import aiohttp
from django.shortcuts import render
from django.http import HttpResponse
from django.core.files.storage import FileSystemStorage
from django.core.cache import cache


def dd_yandex():
    result = cache.get('yandex_data')
    if result is None:
        file_path = os.path.join(os.path.dirname(__file__), 'sources', 'yandex-ru-geo-codes.json')
        with open(file_path, 'r') as f:
            data = json.load(f)
        result = [(item['lr'], item['location']) for item in data]
        cache.set('yandex_data', result, 36000)
    return result


def dd_google():
    result = cache.get('google_data')
    if result is None:
        file_path = os.path.join(os.path.dirname(__file__), 'sources', 'geotargets-google.csv')
        with open(file_path, 'r') as f:
            csv_reader = csv.DictReader(f)
            result = [row['Canonical Name'] for row in csv_reader]
        cache.set('google_data', result, 36000)
    return result


async def upload(request):
    if request.method == 'POST' and request.FILES['file']:
        selected_value1 = request.POST.get('dropdown1')
        selected_value2 = request.POST.get('dropdown2')
        uploaded_file = request.FILES['file']
        if uploaded_file.name.endswith('.xlsx'):
            fs = FileSystemStorage()
            filename = fs.save(uploaded_file.name, uploaded_file)
            uploaded_file_url = fs.url(filename)

            df = pd.read_excel(uploaded_file_url)

            df = df.rename({
                'Запрос': 'search_string',
                'URL': 'url',
            }, axis=1)

            df.loc[:, 'region'] = int(selected_value1)
            df.loc[:, 'location'] = selected_value2
            df = df[['ID', 'url', 'search_string', 'region', 'location']]

            for i in range(len(df)):
                req = df[df.index == i]
                req = req.drop(['ID'], axis=1)
                ya_req = req.loc[:, req.columns != 'location']
                google_req = req.loc[:, req.columns != 'region']
                ya_req = dict(zip(ya_req.columns, ya_req.values[0]))
                google_req = dict(zip(google_req.columns, google_req.values[0]))

                ya_req['return_as_json'] = 1
                google_req['domain'] = 'google.ru'
                google_req['return_as_json'] = 1

                # yandex
                async with aiohttp.ClientSession() as session:
                    async with session.get(url='http://0.0.0.0:5000/process-url/', params=ya_req) as response:
                        ya_response = json.loads(await response.json(encoding='utf-8'))

                # google
                async with aiohttp.ClientSession() as session:
                    async with session.get(url='http://0.0.0.0:5000/search-google/', params=google_req) as response:
                        google_response = json.loads(await response.json(encoding='utf-8'))

                ya_lsi = pd.DataFrame.from_dict(ya_response['lsi'])
                ya_increase_qty = pd.DataFrame(ya_response['увеличить частотность'].values(), index=ya_response['увеличить частотность'].keys(), columns=['увеличить частотность yandex'])
                ya_decrease_qty = pd.DataFrame(ya_response['уменьшить частотность'].values(), index=ya_response['уменьшить частотность'].keys(), columns=['уменьшить частотность yandex'])
                ya_urls = ya_response['обработанные ссылки']

                google_lsi = pd.DataFrame.from_dict(google_response['lsi'])
                google_increase_qty = pd.DataFrame(google_response['увеличить частотность'].values(), index=google_response['увеличить частотность'].keys(), columns=['увеличить частотность google'])
                google_decrease_qty = pd.DataFrame(google_response['уменьшить частотность'].values(), index=google_response['уменьшить частотность'].keys(), columns=['уменьшить частотность google'])
                google_urls = google_response['обработанные ссылки']

                lsi = pd.concat([ya_lsi, google_lsi], axis=0).to_dict()[0]
                increase_qty = pd.merge(ya_increase_qty, google_increase_qty, left_index=True, right_index=True, how='outer').fillna(0, axis=0)
                decrease_qty = pd.merge(ya_decrease_qty, google_decrease_qty, left_index=True, right_index=True, how='outer').fillna(0, axis=0)

                df.loc[i, 'lsi'] = [lsi]
                df.loc[i, 'увеличить частотность yandex'] = [increase_qty.loc[:, 'увеличить частотность yandex'].to_dict()]
                df.loc[i, 'увеличить частотность google'] = [increase_qty.loc[:, 'увеличить частотность google'].to_dict()]
                df.loc[i, 'уменьшить частотность yandex'] = [decrease_qty.loc[:, 'уменьшить частотность yandex'].to_dict()]
                df.loc[i, 'уменьшить частотность google'] = [decrease_qty.loc[:, 'уменьшить частотность google'].to_dict()]
                df.loc[i, 'Yandex выдача'] = [ya_urls]
                df.loc[i, 'Goole выдача'] = [google_urls]

            with pd.ExcelWriter('autorelevant_site/test.xlsx') as writer:
                df.to_excel(writer, sheet_name='test', index=False)

            return HttpResponse(
                f'File uploaded successfully: <a href="{uploaded_file_url}">{uploaded_file_url}</a><br>'
                f'Selected Value 1: {selected_value1}<br>'
                f'Selected Value 2: {selected_value2}'
            )
        else:
            return HttpResponse('Invalid file format. Please upload an XLSX file.')

    search_query1 = request.GET.get('search1', '')
    search_query2 = request.GET.get('search2', '')
    dropdown1_values = [value for value in dd_yandex() if search_query1.lower() in value[1].lower()]
    dropdown2_values = [value for value in dd_google() if search_query2.lower() in value.lower()]
    return render(request, 'upload.html', {
        'dropdown1_values': dropdown1_values,
        'dropdown2_values': dropdown2_values,
        'search_query1': search_query1,
        'search_query2': search_query2,
    })
