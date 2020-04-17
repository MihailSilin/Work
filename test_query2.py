import elasticsearch
# import elasticsearch_dsl as es_dsl
import os
import sys
from sklearn.metrics import classification_report
import json
import re
# -*- coding: utf-8 -*-


BIGRAM_QUERY_TEMPLATE = {
        "query": {
            "bool": {
                "must": [
        {
          "multi_match": {
				"query": "",
				"fuzziness": 1,
				"fields": [
				"norm_phrase"
            ]
          }
        }
      ]
            }
        },
        "size": 5,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
    }

WORD_QUERY_TEMPLATE = {
        "query": {
            "bool": {
                "must": [
        {
          "multi_match": {
				"query": "",
				"fuzziness": 1,
				"fields": [
				"norm_phrase"
            ]
          }
        }
      ]
            }
        },
        "size": 5,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
    }

with open("llt.asc", "r") as f:
	lltLines = f.readlines()
	lltLines  = [x.split('$') for x in lltLines ]
def GetPtId(probablyPT):
	if probablyPT == 'CONCEPT_LESS':
		return 'CONCEPT_LESS'
	for i, line in enumerate(lltLines ):
		#if line.strip() == "":
			#raise ValueError("Empty string #",i)
		if probablyPT == line[0]:
			return line[2] 
		elif probablyPT == line[2]:
			return probablyPT
			
	raise ValueError("PT not found in llt.asc", [probablyPT])

def CheckInTop5(pt_true, hits_list):
	isInTop5 = False
	for hitDict in sorted(hits_list, key=lambda x: x["_score"], reverse=True): 
		if pt_true == hitDict['_source']['pt_id']:
			isInTop5 = True
		break
	if len(hits_list)==0 and pt_true=="CONCEPT_LESS":
		isInTop5 =True
	return isInTop5 

es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
path = "/bigdisk/Mihail/meddra/"
y = []
for i_file, fileName in enumerate(os.listdir(path)):
	if i_file % 50 == 0:
			print("files processed:", i_file)
	with open(path+"/"+fileName, "r") as f:
		for i_line, line in enumerate(f):
			line = re.sub(" {3,}", "\t", line)
			line = line.split("\t")
			phrase = line[-1]
			norm_phrase = list()
			phrase = phrase.replace('\\', '')
			phrase = phrase.replace("'", '')
			phrase = phrase.replace('"', '')
			words = phrase.split(' ')
			for word in words:
				word = word.replace(',', '')
				try:
					parsResult = morph.parse(word)[0]
					lemma = parsResult.normal_form.upper()
					norm_phrase.append(lemma.lower())
				except:
					norm_phrase.append(word.lower())
			if len(norm_phrase) > 1:
				norm_phrase = ' '.join(norm_phrase)
				BIGRAM_QUERY_TEMPLATE["query"]["bool"]["must"][0]["multi_match"]["query"] = norm_phrase
				res = es.search(index = 'en_meddra', body = BIGRAM_QUERY_TEMPLATE)
			else:
				norm_phrase = ' '.join(norm_phrase)
				WORD_QUERY_TEMPLATE["query"]["bool"]["must"][0]["multi_match"]["query"] = norm_phrase
				res = es.search(index = 'en_meddra', body = WORD_QUERY_TEMPLATE)
			
			if "/" in line[1]:
				pt_true = GetPtId(line[1].split("/")[0])
				isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
				y.append(isInTop5)
				pt_true = GetPtId(line[1].split('/')[1].split()[0])
				isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
				y.append(isInTop5)
			elif "+" in line[1]:
				pt_true = GetPtId(line[1].split()[0])
				isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
				y.append(isInTop5)
				pt_true = GetPtId(line[1].split()[2])
				isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
				y.append(isInTop5)
			else:
				pt_true = GetPtId(line[1].split()[0])
				isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
				y.append(isInTop5)
print("accuracy:", sum(y)/len(y))	


				
