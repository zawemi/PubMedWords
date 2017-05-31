#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import nltk
from nltk import *
import psycopg2
from nltk.corpus import stopwords
import re
import pandas as pd
#from nltk import word_tokenize

try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='postgres'")
    conn.autocommit = True
except:
    print("I am unable to connect to the database")

#nltk.download()
#nltk.download("stopwords")

cur_gen = conn.cursor()
cur_gen.execute('select "GeneSymbol" from hgd."GeneAliases" group by "GeneSymbol"')
rows_gen = cur_gen.fetchall()

test = []
word_list = []


#get gene
for row_gen in rows_gen:
    gen_name = row_gen[0]
    print(gen_name)

    words_nbr = pd.DataFrame(columns = ['word', 'nbr'])

    cur_gen_a = conn.cursor()
    cur_gen_a.execute("""SELECT "Alias" FROM hgd."GeneAliases" WHERE "GeneSymbol" = '""" + gen_name + "'")
    rows_gen_a = cur_gen_a.fetchall()

    #get gene alias
    for row_gen_a in rows_gen_a:
        gen_name_a = re.sub(r'[\)\:\.\(]','',row_gen_a[0])
#        gen_name_a = re.sub(r'\)','',gen_name_a1)
#print('--' + gen_name_a)

        cur = conn.cursor()
        cur.execute("""SELECT "ABSTRACT" FROM hgd."MedlineCitation" WHERE textsearchable_index_col @@ to_tsquery('simple','""" + gen_name_a + "')")
        rows = cur.fetchall()
#        print("""SELECT "ABSTRACT" FROM hgd."MedlineCitation" WHERE textsearchable_index_col @@ to_tsquery('simple','""" + gen_name_a + "')")

        #get article
        for row in rows:
            row_text = word_tokenize(row[0])
            words = set(row_text)
            row_len = len(words)
            freq1 = FreqDist(row_text)
            numb = re.compile(r"^[-+]?[0-9]+[.,]?[0-9]*?$")
            punct = re.compile(r"^[\!\"\#\$\%\&\'\(\)\*\+\,\-\.\:\;\<\=\>\?\@\[\\\]\^\_\`\{\|\}\~]$")
        
            filtered_words = [word for word in row_text if word.lower() not in stopwords.words('english')]
            filtered_words2 = [word for word in filtered_words if not numb.match(word)]
            filtered_words4 = [re.sub(r'[\)\:\.\(]','',word) for word in filtered_words2 if not punct.match(word)]
#            filtered_words4 = [re.sub(r'\(','',word) for word in filtered_words3]

            f_words1 = FreqDist(filtered_words4)
            f_words = f_words1.most_common()
#            test = test + most
            df23 = pd.DataFrame(f_words)
            #print(df23)
            #print(f_words)
   
            words_nbr = words_nbr.append(df23, ignore_index=True)
            print(words_nbr)
            
    test2 = pd.DataFrame(words_nbr.groupby(['word'])['nbr'].sum()).reset_index()
    test3 = test2.sort_values(['nbr'], ascending=[False])
    test4 = test3.head(n=50)

#    print(test4)

    for word in test4['word']:
        word_list.append(word)

    genes_words = pd.DataFrame(columns = ['gene', 'words'])
    genes_words.append([gen_name, word_list])
   # print(genes_words)
    #with open('/home/emi/Documents/PubMed/gene_words.csv','a') as file:
    #    genes_words.to_csv(file, header=False)
    
        
#    
#print(genes_words)
