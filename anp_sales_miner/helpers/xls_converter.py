import os

import win32com.client as win32


def convert_xls_to_xlsx(excel_client, input_path, output_path):
    workbook = excel_client.Workbooks.Open(input_path)  # Full path is needed

    if os.path.isfile(output_path):
        os.remove(output_path)

    workbook.SaveAs(output_path, FileFormat=51)
    workbook.Close()


excel = win32.gencache.EnsureDispatch('Excel.Application')

input_path = input('Provide the raw xls full file path: ')
output_path = f'{input_path}x'

try:
    convert_xls_to_xlsx(excel_client=excel, input_path=input_path, output_path=output_path)
except Exception as e:
    print(str(e))
finally:
    excel.Quit()
