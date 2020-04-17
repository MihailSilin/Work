import elasticsearch
# import elasticsearch_dsl as es_dsl
import os
import sys
from sklearn.metrics import classification_report
import json
import re
# -*- coding: utf-8 -*-
from pymorphy2 import MorphAnalyzer
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.metrics import precision_score

morph = MorphAnalyzer()


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
	
N_gram = {
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
	ptLines = f.readlines()
	ptLines  = [x.split('$') for x in ptLines ]
	
def GetPhrase_pt(probablyPT):
	for ptLine in ptLines:
		#print(probablyPT)
		#break
		if probablyPT == ptLine[1]:
			return ptLine[2]
	#return 'CONCEPT_LESS'
			
	raise ValueError("PT not found in pt.asc", [ probablyPT])
	
def CheckInTop5(pt_true, hits_list):
	isInTop5 = None
	for hitDict in sorted(hits_list, key=lambda x: x["_score"], reverse=True): 
		if pt_true == hitDict['_source']['pt_id']:
			isInTop5  = hitDict['_source']['pt_id']
			break
	else:
		isInTop5 = "CONCEPT_LESS"
		
	#if len(hits_list)==0 and pt_true == "CONCEPT_LESS":
		#isInTop5 = "CONCEPT_LESS"
	return isInTop5 


es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
y_true, y_pred = [], []
with open("ADR.txt", "r") as f:
	for i_line, line in enumerate(f):
		line = line.strip()
		line = re.sub(" {3,}", "\t", line)
		line = line.split(";")
		if "???" in line[2]:
			continue
		y_true.append(GetPhrase_pt(line[2]))
		phrase = line[0]
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
			#N_gram["query"]["bool"]["must"][0]["multi_match"]["query"] = norm_phrase
			BIGRAM_QUERY_TEMPLATE["query"]["bool"]["must"][0]["multi_match"]["query"] = norm_phrase
			res = es.search(index='meddra_v4', body = BIGRAM_QUERY_TEMPLATE)
		else:
			norm_phrase = ' '.join(norm_phrase)
			#WORD_QUERY_TEMPLATE["query"]["bool"]["must"]["match"]["norm_phrase"]["query"] = norm_phrase
			WORD_QUERY_TEMPLATE["query"]["bool"]["must"][0]["multi_match"]["query"] = norm_phrase
			res = es.search(index='meddra_v4', body = WORD_QUERY_TEMPLATE)
		if len(res["hits"]["hits"])>0:
			maxHit = sorted(res["hits"]["hits"], key=lambda x: x["_score"])[-1]
			PT_phrase = GetPhrase_pt(maxHit["_source"]["phrase"])
			isInTop5 = CheckInTop5(PT_phrase, res['hits']['hits'])
		else:
			maxHit = {"_source":{"phrase": "CONCEPT_LESS"}}
			PT_phrase = "CONCEPT_LESS"
		#file.write(json.dumps(maxHit["_source"]["phrase"])+"\n")
		y_pred.append(isInTop5)
		#print(y_true)
		#print(y_pred)
		#print(res)
		#break
	report = classification_report(y_true, y_pred, output_dict = True)
	#print(phrase)
	#print(norm_phrase)
	#print(res)
	#break
	print('macro avg', report['macro avg'])
	print('weighted avg', report['weighted avg'])
	print('accuracy', report['accuracy'])
	

