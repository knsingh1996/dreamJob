from pyspark import SparkContext
import re
import sys
import json

#remove special characters and extra spaces
def cleaner(text):
    a = text.replace(r'\r', ' ').replace(r'\t', ' ').replace(r'\n',' ')
    b = re.sub(r'[^A-Za-z0-9 ]+', '', a)
    return re.sub(' +', ' ', b)

#using regex to remove all the tags, but keep the ones with no tags:
def htmlTextToRawText(html):
    
    regex_filter = re.compile('<.*?>')
    raw_text = re.sub(regex_filter, '', html)
    
    return raw_text

if __name__ == "__main__":
    input_file = sys.argv[1] #'raw_data.csv'
    output_file = sys.argv[2] #'clean_group_data.json'
    
    sc = SparkContext('local[*]','Task1')
        
    rdd = sc.textFile(input_file)
    header = rdd.first()
    #CSV, so remove the header, and split on commas
    #Note: We want to split only a maximum of 2 times: id,id,job_title job_description so we can filter out identical job descriptions
    rows = rdd.filter(lambda x: x != header).distinct().map(lambda x: x.split(',',2))
    
    #some data is dirty, there are cases where the line is just a description, or just title. We want to remove those
    rows_complete = rows.filter(lambda x: len(x) > 2)
    #decreases number of lines from 73758 to 72402
    
    #a lot of descriptions are the same, so we filter out to make sure we have distinct ones, 
    #and then split to get title and descriptions separate. Since job descriptions have commas, we set a maximum split of 1
    rows_rel = rows_complete.map(lambda x: x[2]).distinct().map(lambda x: x.split(',',1)).filter(lambda x: len(x)>1)
    #decreases amount of lines from 72402 to 32013. These are the number of distinct job titles, job descriptions pairs
    
    #some of the job descriptions are scraped from web pages, so are in HTML format. We need to remove the html tags if relevant
    
    rows_tagless = rows_rel.map(lambda x:(x[0],htmlTextToRawText(x[1])))
    
    rows_clean = rows_tagless.map(lambda x: (x[0],cleaner(x[1]).lower()))
    
    job_group = rows_clean.groupByKey().filter(lambda x: x[0][0] != ' ').map(lambda x: {'job_name':x[0],'job_description':list(x[1])})
    
    out = job_group.collect()
    with open(output_file, 'w') as fout:
        for o in out:
            out_json = json.dumps(o)
            fout.write(out_json + '\n')
    #job_document = job_group.map(lambda x: (x[0],' '.join(n for n in x[1])))
    
    #job_document_words = job_document.map(lambda x: x[1].split(' '))
    
    #hashingTF = HashingTF()
    #tf = hashingTF.transform(job_document_words)
    
    #tf.cache()
    #idf = IDF().fit(tf)
    #tfidf = idf.transform(tf)
    #tfidf.cache()

