from pyspark import SparkContext
import re
import sys
import json
import math


input_file = 'clean_group_data.json'

sc = SparkContext('local[*]','Task1')
rdd = sc.textFile(input_file).map(lambda x: json.loads(x))
clean_1gram = rdd.map(lambda x: (x['job_name'], [item for sublist in [sublist.split(' ') for sublist in x['job_description']] for item in sublist]))
clean = clean_1gram.map(lambda x: (x[0],[re.sub(r'[0-9\.]+', '', a) for a in x[1]]))
clean = clean.map(lambda x: (x[0],list(filter(lambda v: not (v.isnumeric()),x[1]))) )

def two_gram(one_gram):
    output = []
    for i in range(len(one_gram) - 1):
        output.append((one_gram[i],one_gram[i+1]))
    return output

#number of documents
N = clean.count()

#number of documents word i appears in
clean = clean.map(lambda x: (x[0],two_gram(x[1])))
ni_rdd = clean.map(lambda x: (x[0],list(set(x[1]))))
ni_rdd = ni_rdd.flatMap(lambda x: [(x[0],i) for i in x[1]]).distinct().map(lambda x: (x[1],1)).reduceByKey(lambda a,b: a + b)
#calculate IDF
IDF = ni_rdd.map(lambda x: (x[0],math.log2(N/x[1])))
idf = IDF.collectAsMap()

#calculate TF
def miniCombiner(description):
    count_dic = {}
    for w in description:
        if w in count_dic:
            count_dic[w] += 1
        else:
            count_dic[w] = 1
    max_ = max(count_dic.values()) 
    new_desc = []
    for key in count_dic:
        new_desc.append((key,count_dic[key]/max_))
    return new_desc
        
TF = clean.map(lambda x: (x[0],miniCombiner(x[1]))).persist()

#tfidf
TFIDF = TF.map(lambda x: (x[0],[(i[0],i[1]*idf[i[0]]) for i in x[1]])).map(lambda x: (x[0],sorted(x[1],key=lambda x:x[1],reverse = True)[0:20]))
c = TFIDF.collect()

####################

def word_mapper(ls_jd, keywords):
    out = []
    for k in keywords:
        term = k[0] + ' ' + k[1]
        if term in ls_jd:
            out.append((term,1))
    return out

dic_out = []

for job in c:
    print(job[0])
    jn = job[0]
    jd = [j[0] for j in job[1]]
    rdd_jn = rdd.filter(lambda x: x['job_name'] == jn).flatMap(lambda x: x['job_description'])
    total = rdd_jn.count()
    rdd_jn_map = rdd_jn.flatMap(lambda x: word_mapper(x,jd)).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0],x[1]/total))
    
    prop = rdd_jn_map.collect()
    
    jn_out = {}
    for j in job[1]:
        jn_out[j[0][0] + ' ' + j[0][1]] = {'tfidf_2gram': j[1]}
        
    for p in prop:
        jn_out[p[0]]['proportion'] = p[1]
        
    dic_out.append({'job_name':jn, 'keyword':jn_out})
    
with open('tfidf_2gram.json', 'w') as fout:
    for o in dic_out:
        out_json = json.dumps(o)
        fout.write(out_json + '\n')
