import pymongo

client=pymongo.MongoClient("mongodb://SoSweetWriter:SSW2015@cornichon.lip.ens-lyon.fr:27017/SoSweet")


def insert(tree,id):
    if len(id)==1:
        if id[0] in tree:
            tree[id[0]]+=1
            return True
        else:
            tree[id[0]]=1
            return False
    else:
        if id[0] not in tree:
            tree[id[0]]={}
        return insert(tree[id[0]], id[1:])
        
tree={}
duplicates=[]
nTweets=0
nDuplicates=0
for tweet in client.SoSweet.tweets.find({},{"id":1}, no_cursor_timeout=True):
    nTweets+=1
    if insert(tree, tweet['id']):
        duplicates.append(tweet['id'])
        nDuplicates+=1
        print "%d/%d"%(nDuplicates,nTweets)
    
