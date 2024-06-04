from django.shortcuts import render
from django.http import HttpResponse
from django.core.files.storage import FileSystemStorage

def get_dropdown1_values():
    return ['Option 1', 'Option 2', 'Option 3']

def get_dropdown2_values():
    return ['Choice A', 'Choice B', 'Choice C']


def upload(request):
    if request.method == 'POST' and request.FILES['file']:
        selected_value1 = request.POST.get('dropdown1')
        selected_value2 = request.POST.get('dropdown2')
        uploaded_file = request.FILES['file']
        if uploaded_file.name.endswith('.xlsx'):
            fs = FileSystemStorage()
            filename = fs.save(uploaded_file.name, uploaded_file)
            uploaded_file_url = fs.url(filename)


            return HttpResponse(
                f'File uploaded successfully: <a href="{uploaded_file_url}">{uploaded_file_url}</a><br>'
                f'Selected Value 1: {selected_value1}<br>'
                f'Selected Value 2: {selected_value2}'
            )
        else:
            return HttpResponse('Invalid file format. Please upload an XLSX file.')

    dropdown1_values = get_dropdown1_values()
    dropdown2_values = get_dropdown2_values()
    return render(request, 'upload.html', {
        'dropdown1_values': dropdown1_values,
        'dropdown2_values': dropdown2_values
    })
