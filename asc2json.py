import json
#from ntlk import word_tokenize
from pymorphy2 import MorphAnalyzer
import argparse
import time

morph = MorphAnalyzer()


def normalize_phrase(phrase):
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
    norm_phrase = ' '.join(norm_phrase)
    return norm_phrase

parser = argparse.ArgumentParser(description='Find path for 2 files: llt.asc, pt.asc')
parser.add_argument('-l', type=str,
                        help='llt.asc', default = './llt.asc')
parser.add_argument('-p', type=str,
                        help='pt.asc', default = './pt.asc')

args = parser.parse_args()
ptfilepath = args.p
lltfilepath = args.l


# итоговый json имеет вид: [1:{original:.. norm:.. llt_id:.. pt_id:..} 2:...]
start_time = time.time()
with open(lltfilepath, mode='r', encoding='utf-8') as llt, open(ptfilepath, mode='r', encoding='utf-8') as pt:
    data_pt = {}
    for s in pt:
        data_pt[s[0:s.find('$')]] = s[9:s.find('$', 9)]
    data_llt = []
    for s in llt:
        d = dict()
        d['llt_id'] = s[0:s.find('$')]
        end_of_phrase = s.find('$', 9)
        d['original'] = s[9:end_of_phrase]
        pt_id_begin = end_of_phrase + 1
        d['pt_id'] = s[pt_id_begin:s.find('$', pt_id_begin)]
        d['norm'] = normalize_phrase(d['original'])
        data_llt.append(d)

with open('pt.json', mode='w') as pt, open('llt.json', mode='w') as llt:
    json.dump(data_pt, pt)
    json.dump(data_llt, llt)

print("--- %s seconds ---" % (time.time() - start_time))