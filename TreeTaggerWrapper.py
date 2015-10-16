# -*- coding: utf-8 -*-
# Natural Language Toolkit: Interface to the TreeTagger POS-tagger
#
# Copyright (C) Mirko Otto
# Author: Mirko Otto <dropsy@gmail.com>
# Modified by Jean-Philipe Magué : talk directly to treetagger binary

"""
A Python module for interfacing with the Treetagger by Helmut Schmid.
"""

import os
from subprocess import Popen, PIPE

from nltk.internals import find_binary, find_file
from nltk.tag.api import TaggerI

def tUoB(obj, encoding='utf-8'):
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = unicode(obj, encoding)
    return obj

_treetagger_url = 'http://www.ims.uni-stuttgart.de/projekte/corplex/TreeTagger'

_treetagger_languages = {
u'latin-1':['bulgarian', 'dutch', 'english', 'estonian', 'french', 'german', 'greek', 'italian', 'latin', 'russian', 'spanish', 'swahili'],
u'utf8' : ['french', 'german', 'greek', 'italian', 'spanish']}

"""The default encoding used by TreeTagger: utf8. u'' means latin-1; ISO-8859-1"""
_treetagger_charset = [u'utf8', u'latin-1']

class TreeTagger(TaggerI):
    ur"""
    A class for pos tagging with TreeTagger. The input is the paths to:
     - the path to the TreeTagger binary
     - the path to the parameter file
     - the options

    This class communicates with the TreeTagger binary via pipes.

    Example (no longer valid):

    .. doctest::
        :options: +SKIP

        >>> from treetagger import TreeTagger
        >>> tt = TreeTagger(encoding='latin-1',language='english')
        >>> tt.tag('What is the airspeed of an unladen swallow ?')
        [[u'What', u'WP', u'What'],
         [u'is', u'VBZ', u'be'],
         [u'the', u'DT', u'the'],
         [u'airspeed', u'NN', u'airspeed'],
         [u'of', u'IN', u'of'],
         [u'an', u'DT', u'an'],
         [u'unladen', u'JJ', u'<unknown>'],
         [u'swallow', u'NN', u'swallow'],
         [u'?', u'SENT', u'?']]

    .. doctest::
        :options: +SKIP

        >>> from treetagger import TreeTagger
        >>> tt = TreeTagger()
        >>> tt.tag(u'Das Haus ist sehr schön und groß. Es hat auch einen hübschen Garten.')
        [[u'Das', u'ART', u'd'],
         [u'Haus', u'NN', u'Haus'],
         [u'ist', u'VAFIN', u'sein'],
         [u'sehr', u'ADV', u'sehr'],
         [u'sch\xf6n', u'ADJD', u'sch\xf6n'],
         [u'und', u'KON', u'und'],
         [u'gro\xdf', u'ADJD', u'gro\xdf'],
         [u'.', u'$.', u'.'],
         [u'Es', u'PPER', u'es'],
         [u'hat', u'VAFIN', u'haben'],
         [u'auch', u'ADV', u'auch'],
         [u'einen', u'ART', u'ein'],
         [u'h\xfcbschen', u'ADJA', u'h\xfcbsch'],
         [u'Garten', u'NN', u'Garten'],
         [u'.', u'$.', u'.']]
    """

    def __init__(self, path_to_bin, path_to_param, probabilities=True, threshold=.01, encoding='utf8'):
        """
        Initialize the TreeTagger.

        :param path_to_bin: The TreeTagger binary.
        :param path_to_param: The parameter file
        :param options: options passed to teh treetagger binary
        """
        self._end_of_sentence_tag='<end_of_sentence>'
        self._treetagger_bin = path_to_bin
        self._treetagger_param = path_to_param
        self._treetagger_options = ['-token', '-lemma', '-sgml', '-eos-tag', self._end_of_sentence_tag]
        if probabilities:
            self._treetagger_options += ['-prob', '-threshold', '%f'%threshold]
        self.encoding=encoding

    def tag(self, sentences):
        """Tags a single tokenized sentence or a list of tokenized sentences.
        Sentences are list of tokens;
        The tokens should not contain any newline characters.
        """

        # Write the actual sentences to the temporary input file
        _input = ('\n%s\n'%self._end_of_sentence_tag).join(['\n'.join(s) for s in sentences])

        if isinstance(_input, unicode):
             _input = _input.encode(self.encoding)
        cmd = [self._treetagger_bin] + self._treetagger_options + [self._treetagger_param]
        p = Popen(cmd,stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate(_input)
        treetagger_output = stdout

        # Check the return code.
        if p.returncode != 0:
            print stderr
            raise OSError('TreeTagger command failed!')

        if isinstance(stdout, unicode):
            treetagger_output = stdout.decode(self.encoding)
        else:
            treetagger_output = tUoB(stdout)

        # Output the tagged sentences
        tagged_sentences = []
        for tagged_sentence_string in treetagger_output.strip().split(u'\n%s\n'%self._end_of_sentence_tag):
            tagged_sentence = []
            for tagged_word_string in tagged_sentence_string.split(u'\n'):
                tagged_word_split = tagged_word_string.split(u'\t')
                tagged_word=[tagged_word_split[0]]
                for hypothesis in tagged_word_split[1:]:
                    tagged_word.append(dict(zip(['POS', 'lemma', 'proba'],hypothesis.split(' '))))
                tagged_sentence.append(tagged_word)
            tagged_sentences.append(tagged_sentence)

        return tagged_sentences


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
