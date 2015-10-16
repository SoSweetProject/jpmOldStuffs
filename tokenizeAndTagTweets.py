import glob
import ujson
from happyfuntokenizing import Tokenizer
from TreeTaggerWrapper import TreeTagger

path_to_data='../data/snapshots/2014-10-20/'
files = glob.glob(path_to_data+'2014-1*.data')

tokenizer = Tokenizer(preserve_case=True)
tagger = TreeTagger(path_to_bin='/Users/jmague/Documents/work/treetagger/bin/tree-tagger', path_to_param='/Users/jmague/Documents/work/treetagger/lib/french-utf8.par')


for fileName in files:
    print fileName
    file = open(fileName)
    tweets=[ujson.loads(l) for l in file]
    tokenized_tweets= [tokenizer.tokenize(tweet['tweet']) for tweet in tweets]
    tagged_tweets = tagger.tag(tokenized_tweets)
    for i in range(len(tweets)):
        tweets[i]['tagged_tweet'] = tagged_tweets[i]
    output_file_name = fileName[:-5]+'-tagged.data'
    file = open(output_file_name,'w')
    for tweet in tweets:
        file.write("%s\n"%ujson.dumps(tweet))
