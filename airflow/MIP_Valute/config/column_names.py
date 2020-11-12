source_names = [
    'companies', 'industry_emis', 'industry_naics', 'key_executives'
]
# отсюда указываются имена колонок как в оригинальном файле


columns_paterns = {'Num', 'Country', 'Company', 'Industry (EMIS 14)',
                   'Industry (NAICS)', 'City', 'State/County', 'Postal Code',
                   'Address', 'Phone', 'Fax', 'Email', 'Website',
                   'Address Type', 'Key Executives', 'Business Description/Products',
                   'Number of Employees', 'Employee Reference date',
                   'Financial Auditors', 'Legal Form',
                   'Listed/Unlisted', 'Operational Status',
                   'Shareholders', 'Main Products',
                   'Incorporation Date', 'EMIS ID', 'Total operating revenue',
                   'EBITDA', 'Stock Exchange', 'Fiscal Year',
                   'Audited', 'Consolidated', 'Source', 'Click & Select ID'}
# src_columns указывает какие именно колонки из оригинального файла должны попасть
# в таблицу, которую мы создаем. (Ключи словаря - это и есть таблицы)
src_columns = {
    'companies':  ['Country', 'Company', 'City', 'State/County', 'Postal Code',
                   'Address', 'Phone', 'Fax', 'Email', 'Website', 'Address Type',
                   'Business Description/Products', 'Number of Employees',
                   'Employee Reference date', 'Financial Auditors', 'Legal Form',
                   'Listed/Unlisted', 'Operational Status', 'Shareholders', 'Main Products',
                   'Incorporation Date', 'EMIS ID', 'Total operating revenue', 'EBITDA', 'Stock Exchange',
                   'Fiscal Year', 'Audited', 'Consolidated', 'Source'],
    'industry_emis': ['Industry (EMIS 14)', 'EMIS ID', 'Operational Status', 'Employee Reference date', 'Fiscal Year', 'Audited'],
    'industry_naics': ['Industry (NAICS)', 'EMIS ID', 'Operational Status', 'Employee Reference date', 'Fiscal Year', 'Audited'],
    'key_executives': ['Key Executives', 'EMIS ID', 'Operational Status', 'Employee Reference date', 'Fiscal Year', 'Audited']}
