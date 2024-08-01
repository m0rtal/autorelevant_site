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
from django.conf import settings
import datetime


async def fetch_data(session, url, params):
    async with session.get(url, params=params) as response:
        return json.loads(await response.json(encoding='utf-8'))

async def process_row(sem, row, region, location):
    async with sem:
        req = row.drop(['ID'])
        ya_req = req.drop(['location']).to_dict()
        google_req = req.drop(['region']).to_dict()
        ya_req['region'] = region
        ya_req['return_as_json'] = 1
        google_req['domain'] = 'google.ru'
        google_req['location'] = location
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

        lsi = pd.concat([ya_lsi, google_lsi], axis=0).drop_duplicates()
        increase_qty = pd.merge(ya_increase_qty, google_increase_qty, left_index=True, right_index=True, how='outer').max(axis=1)
        decrease_qty = pd.merge(ya_decrease_qty, google_decrease_qty, left_index=True, right_index=True, how='outer').min(axis=1)

        return {"row": row, "lsi": lsi[0].to_list(), "increase_qty": increase_qty.to_dict(), "decrease_qty": decrease_qty.to_dict(), "ya_urls": ya_urls, "google_urls": google_urls}


def load_data(file_name, key, transform_func):
    result = cache.get(key)
    if result is None:
        file_path = os.path.join(settings.BASE_DIR, 'main', 'sources', file_name)
        with open(file_path, 'r') as f:
            result = transform_func(f)
        cache.set(key, result, 36000)
    return result


def dd_yandex():
    return load_data('yandex-ru-geo-codes.json', 'yandex_data', lambda f: [(item['lr'], item['location']) for item in json.load(f)])

def dd_google():
    return load_data('geotargets-google.csv', 'google_data', lambda f: [row['Canonical Name'] for row in csv.DictReader(f)])

async def upload(request):
    if request.method == 'POST' and request.FILES['file']:
        selected_value1 = request.POST.get('dropdown1')
        selected_value2 = request.POST.get('dropdown2')
        uploaded_file = request.FILES['file']
        if uploaded_file.name.endswith('.xlsx'):
            fs = FileSystemStorage()
            filename = fs.save(uploaded_file.name, uploaded_file)
            uploaded_file_path = fs.path(filename)

            df = pd.read_excel(uploaded_file_path)

            df = df.rename({
                'Запрос': 'search_string',
                'URL': 'url',
            }, axis=1)
            df.loc[:, 'region'] = int(selected_value1)
            df.loc[:, 'location'] = selected_value2
            df = df[['ID', 'url', 'search_string', 'region', 'location']]

            sem = asyncio.Semaphore(2)
            tasks = [process_row(sem, df.loc[i], selected_value1, selected_value2) for i in range(len(df))]
            results = await asyncio.gather(*tasks)

            df = pd.DataFrame(results)
            df = pd.concat([df.drop(['row'], axis=1), df['row'].apply(pd.Series)], axis=1)
            df['lsi'] = df['lsi'].apply(lambda x: '\n'.join(x))
            df['increase_qty'] = df['increase_qty'].apply(lambda x: '\n'.join([f'{k}: {v}' for k, v in x.items()]))
            df['decrease_qty'] = df['decrease_qty'].apply(lambda x: '\n'.join([f'{k}: {v}' for k, v in x.items()]))
            df['ya_urls'] = df['ya_urls'].apply(lambda x: '\n'.join([f'{k}: {v}' for k, v in x.items()]))
            df['google_urls'] = df['google_urls'].apply(lambda x: '\n'.join([f'{k}: {v}' for k, v in x.items()]))
            df = df.rename(columns={
                "lsi": "LSI",
                'search_string': 'Заголовок',
                "url": "Обработанный URL",
                "region": 'Регион',
                "location": "Локация",
                "increase_qty": "Добавить слова",
                "decrease_qty": "Переспам от медианы",
                "ya_urls": "Выдача Яндекса",
                "google_urls": "Выдача Google"
            })
            df = df.reindex(columns=['ID', 'LSI', 'Заголовок', 'Обработанный URL', 'Регион', 'Локация', 'Добавить слова', 'Переспам от медианы', 'Выдача Яндекса', 'Выдача Google'])

            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            output_file_name = f'output_{timestamp}.xlsx'
            output_file_path = os.path.join(os.path.dirname(uploaded_file_path), output_file_name)

            with pd.ExcelWriter(output_file_path) as writer:
                df.to_excel(writer, sheet_name='output', index=False)

            return HttpResponse(
                f'<a href="{fs.url(output_file_name)}">Скачайте результат обработки</a><br>'
                f'<a href="/">Вернуться на главную страницу</a>'
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