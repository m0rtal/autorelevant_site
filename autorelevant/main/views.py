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

            df = pd.read_excel('/home/jollyreap/ML/autorelevant_site/autorelevant' + uploaded_file_url)

            df = df.rename({
                'Запрос': 'search_string',
                'URL': 'url',
            }, axis=1)

            df.loc[:, 'region'] = int(selected_value1)
            df.loc[:, 'location'] = selected_value2
            df = df[['url', 'search_string', 'region', 'location']]

            for i in range(len(df)):
                req = df[df.index == i]
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
                        ya_response = await response.json(encoding='utf-8')

                # google
                async with aiohttp.ClientSession() as session:
                    async with session.get(url='http://0.0.0.0:5000/search-google/', params=google_req) as response:
                        google_response = await response.json(encoding='utf-8')

            print(ya_response)
            print(google_response)

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
