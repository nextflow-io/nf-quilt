
import glob
import os

import spacy

def get_c_path(pathlib, extension):
    path_list = glob.glob(f'{pathlib}/' + f'{extension}')
    return path_list


def run_spacy_nlp(sentence):
    nlp = spacy.load("en_core_web_sm")
    doc = nlp(sentence)
    return [(token.text, token.lemma_, token.pos_, token.tag_,
             token.dep_, token.shape_, token.is_alpha, token.is_stop) for token in doc]
