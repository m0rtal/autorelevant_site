# https://serpapi.com/yandex/yandex-ru-geo-codes.json
# https://xmlstock.com/geotargets-google.csv
import asyncio
import json
import csv
import os
import pandas as pd
import aiohttp
from django.shortcuts import render
from django.http import HttpResponse
from django.core.files.storage import FileSystemStorage
from django.core.cache import cache


async def fetch_data(session, url, params):
    async with session.get(url, params=params) as response:
        return json.loads(await response.json(encoding='utf-8'))


async def process_row(sem, row):
    async with sem:
        req = row.drop(['ID'])
        ya_req = req.drop(['location']).to_dict()
        google_req = req.drop(['region']).to_dict()
        ya_req['return_as_json'] = 1
        google_req['domain'] = 'google.ru'
        google_req['return_as_json'] = 1

        async with aiohttp.ClientSession() as session:
            ya_response = await fetch_data(session, 'http://0.0.0.0:5000/process-url/', ya_req)
            google_response = await fetch_data(session, 'http://0.0.0.0:5000/search-google/', google_req)

        ya_lsi = pd.DataFrame.from_dict(ya_response['lsi'])
        ya_increase_qty = pd.DataFrame(ya_response['увеличить частотность'].values(), index=ya_response['увеличить частотность'].keys(), columns=['увеличить частотность yandex'])
        ya_decrease_qty = pd.DataFrame(ya_response['уменьшить частотность'].values(), index=ya_response['уменьшить частотность'].keys(), columns=['уменьшить частотность yandex'])
        ya_urls = ya_response['обработанные ссылки']

        google_lsi = pd.DataFrame.from_dict(google_response['lsi'])
        google_increase_qty = pd.DataFrame(google_response['увеличить частотность'].values(), index=google_response['увеличить частотность'].keys(), columns=['увеличить частотность google'])
        google_decrease_qty = pd.DataFrame(google_response['уменьшить частотность'].values(), index=google_response['уменьшить частотность'].keys(), columns=['уменьшить частотность google'])
        google_urls = google_response['обработанные ссылки']

        lsi = pd.concat([ya_lsi, google_lsi], axis=0).transpose().to_dict()[0]
        increase_qty = pd.merge(ya_increase_qty, google_increase_qty, left_index=True, right_index=True, how='outer').fillna(0, axis=0).transpose()
        decrease_qty = pd.merge(ya_decrease_qty, google_decrease_qty, left_index=True, right_index=True, how='outer').fillna(0, axis=0).transpose()

        row.loc['lsi'] = [lsi]
        row.loc['увеличить частотность yandex'] = [increase_qty.loc[:, 'увеличить частотность yandex'].to_dict()]
        row.loc['увеличить частотность google'] = [increase_qty.loc[:, 'увеличить частотность google'].to_dict()]
        row.loc['уменьшить частотность yandex'] = [decrease_qty.loc[:, 'уменьшить частотность yandex'].to_dict()]
        row.loc['уменьшить частотность google'] = [decrease_qty.loc[:, 'уменьшить частотность google'].to_dict()]
        row.loc['Yandex выдача'] = [ya_urls]
        row.loc['Goole выдача'] = [google_urls]

        return row


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

            df = pd.read_excel('/home/jollyreap/ML/autorelevant_site/autorelevant' + uploaded_file_url)
            df = df.rename({
                'Запрос': 'search_string',
                'URL': 'url',
            }, axis=1)
            df.loc[:, 'region'] = int(selected_value1)
            df.loc[:, 'location'] = selected_value2
            df = df[['ID', 'url', 'search_string', 'region', 'location']]

            sem = asyncio.Semaphore(2)  # ограничить пока до 2 задач
            tasks = [process_row(sem, df.loc[i], selected_value1, selected_value2) for i in range(len(df))]
            df = pd.DataFrame(await asyncio.gather(*tasks))

            with pd.ExcelWriter('/home/jollyreap/ML/autorelevant_site/test.xlsx') as writer:
                df.to_excel(writer, sheet_name='test', index=False)

            return HttpResponse(
                f'File uploaded successfully: <a href="{uploaded_file_url}">{uploaded_file_url}</a><br>'
                f'Selected Value 1: {selected_value1}<br>'
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

