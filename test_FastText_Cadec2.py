# -*- coding: utf8 -*-
  
import json
import numpy as np
import os
from gensim.models.wrappers import FastText
from tqdm import tqdm
import sys
from sklearn.metrics import classification_report, precision_recall_fscore_support
import json
import re
import gensim
from pymorphy2 import MorphAnalyzer
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.metrics import precision_score

morph = MorphAnalyzer()

def cosine(u, v):
    return np.dot(u, v)/(np.linalg.norm(u)*np.linalg.norm(v))
	

FT_PATH = '/bigdisk/Mihail/'
print('Initializing FastText model')
fasttext_model = os.path.join(FT_PATH, 'cc.en.300')
ft_model = FastText.load_fasttext_format(fasttext_model)


# ft_model= gensim.models.FastText.load_fasttext_format('/bigdisk/Mihail/ft_native_300_ru_wiki_lenta_lemmatize', binary=False)
# ft_model.save_word2vec_format('/bigdisk/Mihail/ft_native_300_ru_wiki_lenta_lemmatize'+".bin", binary=True)
# ft_model = gensim.models.FastText.load_fasttext_format('/bigdisk/Mihail/ft_native_300_ru_wiki_lenta_lemmatize'+".bin", binary=True)

print("llt from Meddra in FastText")
meddra_model = []
id_words = []
with open("llt.asc", "r") as f:
	lltLines = f.readlines()
	lltLines  = [x.split('$') for x in lltLines ]
	for line in tqdm(lltLines):
		try:
			el_model_vec = ft_model[line[1]]
			meddra_model.append(el_model_vec)
			id_words.append(line[2])
		except:
			pass


# def GetPhrase_pt(probablyPT):
	# pt_id = None
	# for line in lltLines:
		# if probablyPT == line[1]:
			# pt_id = line[2]
			# break
	# else:
		# pt_id = "CONCEPT_LESS"
	# return pt_id

	# raise ValueError("PT not found in pt.asc", [probablyPT])
	
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
	

print("phrase from Cadec in FastText")
path = "/bigdisk/Mihail/meddra/"
cadec_model = []
y_true, y_pred = [], []
max_vector = []
for i_file, fileName in tqdm(enumerate(os.listdir(path))):
	if i_file % 50 == 0:
		print("files processed:", i_file)
	with open(path+"/"+fileName, "r") as f:
		for i_line, line in enumerate(f):
			id_counter = 0
			line = re.sub(" {3,}", "\t", line)
			line = line.split("\t")
			phrase = line[-1]
			norm_phrase = list()
			phrase = phrase.replace('\\', '')
			phrase = phrase.replace('\n', '')
			phrase = phrase.replace("'", '')
			phrase = phrase.replace('"', '')
			phrase = phrase.strip("\t")
			words = phrase.split(' ')
			for word in words:
				word = word.replace(',', '')
				try:
					parsResult = morph.parse(word)[0]
					lemma = parsResult.normal_form.upper()
					norm_phrase.append(lemma.lower())
				except:
					norm_phrase.append(word.lower())
			norm_phrase = ' '.join(norm_phrase)
			try:
				elemCadec = ft_model[norm_phrase]
				print(norm_phrase)
			except:
				pass
				# Nvector = []
				# norm_phrase = re.sub("\d", "", norm_phrase)
				# norm_phrase = re.sub("/", "", norm_phrase)
				# norm_phrase = norm_phrase.split()
				# for Nphrase in norm_phrase:
					# Nvector.append(ft_model[Nphrase])
				# elemCadec = np.mean(Nvector)
				# np.transpose(elemCadec)
				# elemCadec = elemCadec[1:300]
				# print(elemCadec)
				# print(fileName)
			# print(norm_phrase)
			if "/" in line[1]:
				pt_true = GetPtId(line[1].split("/")[0])
				y_true.append(pt_true)
				id_counter += 1
				pt_true = GetPtId(line[1].split('/')[1].split()[0])
				y_true.append(pt_true)
				id_counter += 1
			elif "+" in line[1]:
				pt_true = GetPtId(line[1].split()[0])
				y_true.append(pt_true)
				id_counter += 1
				pt_true = GetPtId(line[1].split()[2])
				y_true.append(pt_true)
				id_counter += 1
			else:
				pt_true = GetPtId(line[1].split()[0])
				y_true.append(pt_true)
				id_counter += 1
				
			max_sim = -100
			id_max_sim = ''
			for i_id, elemMeddra in zip(id_words, meddra_model):
				sim = cosine(elemCadec, elemMeddra)
				if sim > max_sim:
					max_sim = sim
					id_max_sim = i_id
			y_pred.append(id_max_sim)
			if id_counter == 2:
				y_pred.append(id_max_sim)
			# if len(y_true) > len(y_pred):
				# print(fileName)
			# break
			# print(y_true)
			# print(y_pred)
			# break
					
 
	
	"""
	это точность для терминов кадека для всех фраз с векторами,
	полученными просто подавая всю сущность в модель FT.
	
	"""
	
report = classification_report(y_true, y_pred, output_dict = True)
print('macro avg', report['macro avg'])
print('weighted avg', report['weighted avg'])
print('accuracy', report['accuracy'])





