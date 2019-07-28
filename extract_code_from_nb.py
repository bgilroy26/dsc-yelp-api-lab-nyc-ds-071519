import json

with open('index.ipynb') as f:
    cells = json.loads(f.read())['cells']

for cell in cells:
    if cell['cell_type'] == 'code':
        f = open('yelp_data.py','a')
        f.write(''.join(cell['source']))
        f.write('\n')

