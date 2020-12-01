import json
import sys
import requests

url = 'REDACTED'

#load in data 
tfidf = []
tfidf_2gram = []
with open('tfidf.json', 'r') as f:
    for line in f:
        tfidf.append(json.loads(line))
    
with open('tfidf_2gram.json', 'r') as f:
    for line in f:
        tfidf_2gram.append(json.loads(line))
        
keywords = {}
for job in tfidf:
    for w in job['keyword']:
        if w not in keywords:
            keywords[w] = {}
        keywords[w][job['job_name']] = job['keyword'][w]
        
for job in tfidf_2gram:
    for w in job['keyword']:
        if w not in keywords:
            keywords[w] = {}
        keywords[w][job['job_name']] = job['keyword'][w]['proportion']

count = 0;
for w in keywords:
    for k in keywords[w]:
        out_json = {'word':w.lower(),'job': k, 'proportion': keywords[w][k]['proportion']}
        requests.patch(url+'keywords/' + str(count) + '.json', json.dumps(out_json))
        print(str(count))
        count += 1
        
reformatted_jd = {}
for job in tfidf:
    temp = [];
    for key in job['keyword']:
        temp.append((key, job['keyword'][key]['tfidf'], job['keyword'][key]['proportion']))
    reformatted_jd[job['job_name']] = temp
    
#for job in tfidf_2gram:
#    temp = [];
#    for key in job['keyword']:
#        temp.append((key, job['keyword'][key]['tfidf_2gram'], job['keyword'][key]['proportion']))
#    reformatted_jd[job['job_name']].extend(temp)

signature_jd = {} 
for k in reformatted_jd:
    reformatted_jd[k].sort(key = lambda x: x[1], reverse = True)
    signature_jd[k] = {}
    for i in range(10):
        trip = reformatted_jd[k][i]
        signature_jd[k][i] = {'word': trip[0], 'prop':trip[2]}
        
for w in signature_jd:
        requests.patch(url+'signature/' + w.lower() + '.json', json.dumps(signature_jd[w]))
