import elasticsearch
# import elasticsearch_dsl as es_dsl
import os
import sys
from sklearn.metrics import classification_report, precision_recall_fscore_support
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
	
def CheckInTop5(pt_true, hits_list):
	isInTop5 = None
	for hitDict in sorted(hits_list, key=lambda x: x["_score"], reverse=True): 
		if pt_true == hitDict['_source']['pt_id']:
			isInTop5  = hitDict['_source']['pt_id']
			break
	else:
		isInTop5 = "CONCEPT_LESS"
		
	if len(hits_list)==0 and pt_true == "CONCEPT_LESS":
		isInTop5 = "CONCEPT_LESS"
	return isInTop5 
	
 
es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
path = "/bigdisk/Mihail/meddra/"
with open("sort.txt", "w") as file:
	y_true, y_pred = [], []
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
					res = es.search(index='en_meddra', body=BIGRAM_QUERY_TEMPLATE)
				else:
					norm_phrase = ' '.join(norm_phrase)
					WORD_QUERY_TEMPLATE["query"]["bool"]["must"][0]["multi_match"]["query"] = norm_phrase
					res = es.search(index='en_meddra', body=WORD_QUERY_TEMPLATE)
					
				if "/" in line[1]:
					pt_true = GetPtId(line[1].split("/")[0])
					file.write(str(pt_true) + "\n")
					y_true.append(pt_true)
					pt_true = GetPtId(line[1].split('/')[1].split()[0])
					file.write(str(pt_true) + "\n")
					y_true.append(pt_true)

				elif "+" in line[1]:
					pt_true = GetPtId(line[1].split()[0])
					file.write(str(pt_true) + "\n")
					y_true.append(pt_true)
					pt_true = GetPtId(line[1].split()[2])
					file.write(str(pt_true) + "\n")
					y_true.append(pt_true)
				else:
					pt_true = GetPtId(line[1].split()[0])
					file.write(str(pt_true) + "\n")
					y_true.append(pt_true)
					
					

				#res = es.search(index='en_meddra', body=BIGRAM_QUERY_TEMPLATE)
				if len(res["hits"]["hits"])>0:
					maxHit = sorted(res["hits"]["hits"], key=lambda x: x["_score"])[-1]
				else:
					maxHit = {"_source":{"pt_id": "CONCEPT_LESS"}}
				file.write(json.dumps(maxHit["_source"])+"\n")

				if "/" in line[1]:
					pt_true = GetPtId(line[1].split("/")[0])
					isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
					y_pred.append(isInTop5)
					pt_true = GetPtId(line[1].split('/')[1].split()[0])
					isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
					y_pred.append(isInTop5)
				elif "+" in line[1]:
					pt_true = GetPtId(line[1].split()[0])
					isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
					y_pred.append(isInTop5)
					pt_true = GetPtId(line[1].split()[2])
					isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
					y_pred.append(isInTop5)
				else:
					isInTop5 = CheckInTop5(pt_true, res['hits']['hits'])
					y_pred.append(isInTop5)

	y_true, y_pred = [yt for yt in y_true if yt!="CONCEPT_LESS"], [yp for yt, yp in zip(y_true, y_pred) if yt!="CONCEPT_LESS"]
	report = classification_report(y_true, y_pred, output_dict = True)
	print('macro avg', report['macro avg'])
	print('weighted avg', report['weighted avg'])
	print('accuracy', report['accuracy'])
	#print('NO_MATCH', report['NO_MATCH'])
	#print('CONCEPT_LESS', report['CONCEPT_LESS'])
	equal = 0
	for yt, yp in zip(y_true, y_pred):
		if yt == yp:
			equal+=1
	print("MyAccuracy:", equal / len(y_true))
	print("Micro:")
	report = precision_recall_fscore_support(y_true, y_pred, average="micro")
	print(report )



				

			
