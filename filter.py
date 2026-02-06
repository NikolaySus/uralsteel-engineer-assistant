import json
data = json.load(open('input.json', 'r', encoding='utf-8'))
paths = [d['content_length'] for d in data['statuses']['processed'] if d.get('status') == 'processed']
for p in paths: print(p)