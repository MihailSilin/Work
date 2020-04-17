import json
import copy
from pymorphy2 import MorphAnalyzer
import elasticsearch
import elasticsearch_dsl as es_dsl
import logging
import os
import re
from difflib import SequenceMatcher
import pandas as pd
import argparse
from transliterate import translit
import time
import string
import sys

parser = argparse.ArgumentParser(description='For primary json file path')
parser.add_argument('-p', '--path', type=str, default='./AIS_Roszdravnadzora_ezmap.json', help='Primary json file path')
args = parser.parse_args()

def similar(a, b):
    # ratcliff
    return SequenceMatcher(None, a, b).ratio()

morph = MorphAnalyzer()


def normalize_text(inp_text):
    if (str(inp_text) == ''):
        inp_text = ""
    l_sent = []
    words = inp_text.replace(',', '')
    words = words.split(' ')
    for word in words:
        try:
            parsResult = morph.parse(word)[0]
            lemm = parsResult.normal_form.upper()
            pos = parsResult.tag.POS
            d_word = {"form": word,
                      "lemma": lemm,
                      "pos": pos,
                      "grm": ""}
            l_sent.append(d_word)
        except Exception as e:
            raise (e)
            # logger.error("Слово без леммы: {0}".format(word))
    return [l_sent]

QUERY_TEMPLATE = {
        "query": {
                "bool": {
                    "must": {
                        "bool": {
                            "must": [
                                {"match":
                                    {
                                        "norm_phrase": {
                                            "query": "",
                                            "fuzziness": 1,
                                    },
                                }}

                            ]}},
                    "should": [
                        {"match": {
                            "norm_phrase.bigrams": ""
                        }}
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
                "must": {
                    "match": {
                        "norm_phrase": {
                            "query": "",
                            "fuzziness": 1,
                        },
                    }
                }
            }
        },
        "size": 5,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
    }
SMALL_WORD_QUERY_TEMPLATE = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "norm_phrase": {
                            "query": "ABGs normal",
                        },
                    }
                }
            }
        },
        "size": 5,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
    }
BIGRAM_QUERY_TEMPLATE = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "norm_phrase.bigrams": {
                            "query": "",
                            "fuzziness": 1,
                        },
                    }
                }
            }
        },
        "size": 5,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
    }

QueryForProbableRequestsByLemms_1 = {
        "query": {
            "bool": {
                "should": [
                    {
                        "constant_score": {
                            "query": {
                                "match": {
                                    "request_lemms": "LEMMA" }}
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
       }
    }

def query_body_from_str(text_in, template="word"):
    """
        Генерируем тело запроса к ElasticSearch по строке

        Arguments:
        template : str
            word == простой поиск только по словам
            bigram == поиск только по биграммам
    """

    

    text_in = text_in.replace("\n", " ")
    norm_text = normalize_text(text_in)
    list_lemm = [line["lemma"].strip() for line in norm_text[0]]
    text_norm = " ".join(list_lemm)

    text_in = re.sub("\W+", " ", text_in)
    text_in = re.sub(" +", " ", text_in)
    text_in = re.sub("^НЕ ", "НЕ_", text_in, re.I | re.U)
    text_in = re.sub("([^а-яА-ЯёЁ])НЕ ", "\\1НЕ_", text_in, re.I | re.U)
    text_in = text_in + " | " + " ".join(text_in.split()[::-1])

    text_norm = text_norm.lower()
    if template=="word":
        body = WORD_QUERY_TEMPLATE
        body["query"]["bool"]["must"]["match"]["norm_phrase"]["query"] = text_norm
    elif template=="small_word":
        body = SMALL_WORD_QUERY_TEMPLATE
        body["query"]["bool"]["must"]["match"]["norm_phrase"]["query"] = text_norm
    elif template=="bigram":
        body = BIGRAM_QUERY_TEMPLATE
        body["query"]["bool"]["must"]["match"]["norm_phrase.bigrams"]["query"] = text_norm
    return body

def rus_or_en(word): #возвращает True, если rus, иначе false, слово считается rus, если в нем хотя бы одна буква не соответствует en алфавиту
    word = word.lower()
    en_alphabet = 'abcdefghijklmnopqrstuvwxyz'
    for char in word:
        if (char.isalpha()):
            for en_char in en_alphabet:
                if (char != en_char):
                    break
            else:
                return False #выход из цикла был совершен без помощи break - значит все буквы принадлежат en алфавиту
    return True


def rusification(word):
    if not rus_or_en(word): #если слово английское
        word = translit(word, 'rus')
    return word

'''
Здесь начинается основная часть
'''
def test_res(res, p_adr, n_adr):
    goodFind = False
    max_score = res['hits']['max_score']
    if (max_score is not None) and (max_score > 6):
        normalized_adr = res['hits']['hits'][0]['_source']['phrase']
        # print(res['hits']['hits'][0]['_score'])
        """
        if (n_adr>len(li_thr)):
            thr = 40
            print('There is the word %s with length %s'%(p_adr, n_adr))
        else:
            thr = li_thr[n_adr-1]
        """
        if n_adr == 1:
            if len(normalized_adr.split()) == 1:
                goodFind = True
        else:
            if similar(p_adr, res['hits']['hits'][0]['_source']['phrase']) > 0.9:
                # if len(res['hits']['hits']) == 1:
                goodFind = True
                # print("Only one hit for <{}>: {}".format(p_adr, res['hits']['hits'][0]['_source']['phrase']))
                # else:
                #     scoresRatio = res['hits']['hits'][1]['_score'] / res['hits']['hits'][0]['_score']
                #     if scoresRatio < 0.9:
                #         goodFind = True
    return goodFind

def search_push(adr, n_adr, es, match_df, mes, no_result_df, field, additional_inf):
    if n_adr == 1:
        if len(adr)<=4:
            query_body = query_body_from_str(adr, template="small_word")
        else:
            query_body = query_body_from_str(adr, template="word")
    else:
        query_body = query_body_from_str(adr, template="bigram")
    res = es.search(index='meddra_v4', body=query_body)
    # rus_adr = rusification(adr) можно для репортов на английском сделать русификацию слов транслитом и искать по rus_adr,
    # добавля DataFrame либо оригинальный adr, либо adr + '|' + rus_adr
    # но пока непонятно, что делать с иностранными репортами вообще
    goodFind = test_res(res, adr, n_adr)
    if (goodFind):
        shape = no_result_df.shape
        match_df.loc[shape[0]] = [mes['ИЗВЕЩЕНИЕ О НЕБЛАГОПРИЯТНОЙ РЕАКЦИИ (НР) ЛЕКАРСТВЕННОГО СРЕДСТВА']['Сообщение'],
                           field,
                           adr,
                           str(res['hits']['hits'][0]['_source']['phrase']),
                           str(res['hits']['hits'][0]['_source']['llt_id']),
                           str(res['hits']['hits'][0]['_source']['pt_cat']),
                           str(res['hits']['hits'][0]['_source']['pt_id'])]
        print('%s is in Meddra'%adr)
    else:
        top = ['-' for i in range(5)]
        i_hit = -1
        for i_hit, hit in enumerate(res['hits']['hits']):
            if i_hit == 5:
                break
            if hit is not None:
                top[i_hit] = hit['_source']['phrase'] + '|' + hit['_source']['llt_id']
        if i_hit==-1 and n_adr>1: #меняем на word template
            query_body = query_body_from_str(adr, template="word")
            res = es.search(index='meddra_v4', body=query_body)
            for i_hit, hit in enumerate(res['hits']['hits']):
                if i_hit == 5:
                    break
                if hit is not None:
                    top[i_hit] = hit['_source']['phrase'] + '|' + hit['_source']['llt_id']
        shape = no_result_df.shape
        weight = 'Неизвестно' if int(mes['СВЕДЕНИЯ О ПАЦИЕНТЕ']['Вес (кг)']) == 0 else int(mes['СВЕДЕНИЯ О ПАЦИЕНТЕ']['Вес (кг)'])
        additional_inf = ''.join([char for char in additional_inf if ord(char) > 31 or ord(char) == 9])
        if additional_inf == '':
            additional_inf = 'Отсутствует'
        no_result_df.loc[shape[0]] = [mes['ИЗВЕЩЕНИЕ О НЕБЛАГОПРИЯТНОЙ РЕАКЦИИ (НР) ЛЕКАРСТВЕННОГО СРЕДСТВА']['Сообщение'],
                               field,
                               mes['СВЕДЕНИЯ О ПАЦИЕНТЕ']['Пол'],
                               # mes['СВЕДЕНИЯ О ПАЦИЕНТЕ']['Вес (кг)'],
                               weight,
                               mes['СВЕДЕНИЯ О ПАЦИЕНТЕ']['Возраст'],
                               # mes['ДОПОЛНИТЕЛЬНЫЕ СВЕДЕНИЯ']['Значимая доп. информация'],
                               additional_inf,
                               # '',
                               adr,
                               top[0],
                               top[1],
                               top[2],
                               top[3],
                               top[4],
                               ''
                               ]
        print('%s is not in Meddra'%adr)
    return True


def garbage_guard(word, garbage_df, mes_id):
    origin_word = word
    word = re.sub(r"nan", "", word)
    word = re.sub(r"[Нн][еа]\s+|[Пп]ри\s+|[Вв]\s+|[Дд]ля\s+|[Бб]ез\s+", "", word)
    word = re.sub(r"[Нн][еа]$|[Пп]ри$|[Вв]$|[Дд]ля$|[Бб]ез$", "", word)
    word = re.sub(r"\d+", "", word)
    word = re.sub(r'[.,"()\+/–-]+', '', word)
    word = re.sub(r'\s+', '', word)
    if word == '' or len(origin_word)==1:
        shape = garbage_df.shape
        garbage_df.loc[shape[0]] = [mes_id, origin_word]
        return True
    return False

def create_table(primary_json_path, entity_json_path, es):
    with open(primary_json_path) as p_f:
        primary_data = json.load(p_f)
    with open(entity_json_path) as e_f:
        entity_data = json.load(e_f)
    match_df = pd.DataFrame(columns = ('Сообщение', 'Поле', 'Изначальное слово', 'Его llt категория', 'llt_id', 'Его pt категория', 'pt_id'))
    no_result_df = pd.DataFrame(columns = ('Сообщение', 'Поле', 'Пол',
                                 'Вес', 'Возраст',  'Доп. информация', 'Изначальное слово',  'Топ_1', 'Топ_2', 'Топ_3', 'Топ_4', 'Топ_5', 'Верный номер из Топ\'а'))
    garbage_df = pd.DataFrame(columns = ('Сообщение', 'Мусор'))
    for i, mes in enumerate(primary_data):
        # if i==200:
        #     break
        print('-----\n', '~Searching in field clinical narrative...')
        field = 'Дополнительная информация'
        entities = {}
        mes_id = mes['ИЗВЕЩЕНИЕ О НЕБЛАГОПРИЯТНОЙ РЕАКЦИИ (НР) ЛЕКАРСТВЕННОГО СРЕДСТВА']['Сообщение']
        for j, e_mes in enumerate(entity_data):
            if (e_mes['meta']['ID'] == mes_id):
                #Настина функция
                label2Idx = {
                    'Disease:DisTypeDiseasename': [],
                    'Disease:DisTypeIndication': [],
                    'ADR': [],
                    'Disease:DisTypeNegatedADE': [],
                    'Disease:DisTypeADE-Neg': [],
                }
                for i_s, sentences in enumerate(e_mes['sentences']):
                    for i_f, words in enumerate(sentences):
                        predict_tag = words['predict']
                        if predict_tag != ['O']:
                            for p in predict_tag[0].split(', '):
                                if p[2:] in label2Idx.keys():
                                    if p[0] == 'B' and label2Idx[p[2:]] == []:
                                        label2Idx[p[2:]].append([words['forma'], i_f])
                                    if p[0] == 'I':
                                        label2Idx[p[2:]].append([words['forma'], i_f])
                                    if p[0] == 'B' and label2Idx[p[2:]] != []:
                                        label_list = [i[0] for i in label2Idx[p[2:]]]
                                        entities[' '.join([str(i_s), str(i_f), '|', p[2:]])] = label_list
                                        label2Idx[p[2:]] = []
                                        label2Idx[p[2:]].append([words['forma'], i_f])
                for p in label2Idx:
                     if p != []:
                         label_list = [i[0] for i in label2Idx[p]]
                         entities[' '.join([str(i_s), str(i_f), '|', p])] = label_list
                additional_inf = e_mes['text']
                break
        for i_entity in entities.values():
            words_iter = filter(lambda x: re.sub('[.,"()–-]+|\s+', '', x) != '', i_entity)
            ent = ' '.join(list(words_iter))
            ent = ent.strip()
            n_ent = len(ent.split(' '))
            if (garbage_guard(ent, garbage_df, mes_id)):
                continue
            search_push(ent, n_ent, es, match_df, mes, no_result_df, field, additional_inf)
        print('-----\n', '~Searching in field reaction...')
        field = 'Описание реакции (MedDRA)'
        for i_adr in mes['СВЕДЕНИЯ О НР']:
            words_iter = filter(lambda x: re.sub('[.,"()–-]+|\s+', '', x) != '', str(i_adr['Описание реакции (MedDRA)']).split(' '))
            adr = ' '.join(list(words_iter))
            # adr = str(i_adr['Описание реакции (MedDRA)']).strip()
            if (garbage_guard(adr, garbage_df, mes_id)):
                continue
            n_adr = len(adr.split(' '))
            search_push(adr, n_adr, es, match_df, mes, no_result_df, field, additional_inf)
        print('-----\n','~Searching in field indication...')
        field = 'Показание'
        for i_indication in mes['СВЕДЕНИЯ О ЛЕКАРСТВЕННЫХ СРЕДСТВАХ']:
            words_iter = filter(lambda x: re.sub('[.,"()–-]+|\s+', '', x) != '', str(i_indication['Показание']).split(' '))
            ind = ' '.join(list(words_iter))
            # ind = str(i_indication['Показание']).strip()
            if (garbage_guard(ind, garbage_df, mes_id)):
                continue
            n_ind = len(ind.split(' '))
            search_push(ind, n_ind, es, match_df, mes, no_result_df, field, additional_inf)
        n_rows_match = match_df.shape
        n_rows_match = n_rows_match[0]
        n_rows_nores = no_result_df.shape
        n_rows_nores = n_rows_nores[0]
        n_garbage = garbage_df.shape
        n_garbage = n_garbage[0]
    print('-----\n','~The amount of rows in match_df.xls is %s\n'%n_rows_match, '~The amount of rows in no_res_df.xls is %s\n'%n_rows_nores, '~The amount of garbage is %s'%n_garbage)
    return (match_df, no_result_df, garbage_df)

def main():
    es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    match_df, no_result_df, garbage_df = create_table(args.path, 'table_match_GR.json', es)
    match_df.to_excel(excel_writer='match_df.xlsx', encoding='utf-8')
    no_result_df.to_excel(excel_writer='no_result_df.xlsx', encoding='utf-8')
    garbage_df.to_excel(excel_writer='garbage_df.xlsx', encoding='utf-8')
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Program has finished in --- %s seconds ---" % (time.time() - start_time))
