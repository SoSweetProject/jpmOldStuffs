import json
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import networkx as nx
import datetime

def frange(start, stop, step):
    l = []
    while start < stop:
        l.append(start)
        start += step

    return l

def plot_ecdf(x):
    """ Plots the distribution function of the data in x in the current subplot """
    x = np.array(x, copy=True)
    x.sort()
    nobs = len(x)
    y = np.arange(nobs) / float(nobs)
    plt.plot(x, y )

def plot_pdf_int(data,style):
    """ Plots the density function of the data in the current subplot """
    bins = {}
    nelts = len(data)
    for elt in data:
        if elt in bins :
            bins[elt] += 1
        else:
            bins[elt] = 1
    for k in bins:
        bins[k] = float(bins[k])/float(nelts)

    sorted_bins = bins.items()
    sorted_bins.sort()

    x = map(lambda data: data[0], sorted_bins)
    y = map(lambda data: data[1], sorted_bins)
    plt.plot(x,y,style)

def plot_pdf_float(data, nbins):
    plt.hist(data, nbins, normed = True)

def plot_gaussian(start, stop, npoints, mean, std):
    step = float(stop - start) / float(npoints)
    x = map(lambda x: x + step/2.0, frange(start, stop, step))

    plt.plot(x, stats.norm.pdf(x, mean, std), 'g-o')

def print_stat(data):
    """ Prints on stdout the basic statistics about the array data """
    n, min_max, mean, var, skew, kurt = stats.describe(data)
    sd = np.std(data)
    me = np.median(data)
    print("Number of elements: {0:d}".format(n))
    print("Minimum: {0:8.6f} Maximum: {1:8.6f}".format(min_max[0], min_max[1]))
    print("Mean: {0:8.6f}".format(mean))
    print("Variance: {0:8.6f}".format(var))
    print("Skew : {0:8.6f}".format(skew))
    print("Kurtosis: {0:8.6f}".format(kurt))
    print("Std: {0:8.6f}".format(sd))
    print("Median: {0:8.6f}".format(me))

def plot_stat(data, log, saving, label):
    """ Plots the density functions of the data

    If log is true, the scaling will be logarithmic.
    If saving countains the name of a file, the resulting figure will be saved in this file

    """

    plt.figure()

    plot_pdf_int(data,'bo')                #fonction de distribution
    plt.xlabel(label)
    plt.ylabel('Probability')
    if log:
        plt.xscale('log')
        plt.yscale('log')

    if saving:
        f=open(saving,'w')
        plt.savefig(f,format="pdf")
        f.close()
    else:
        plt.show()
        plt.close()

def plot_stat_float(data, log, nbins, saving, label):
    plt.figure()

    plot_pdf_float(data, nbins)
    plot_gaussian(min(data), max(data), nbins, np.mean(data), np.std(data))

    plt.xlabel(label)
    plt.ylabel('Probability')
    if log:
        plt.xscale('log')
        plt.yscale('log')

    if saving:
        with open(saving, 'w') as f:
            plt.savefig(f, format = "pdf")
    else:
        plt.show()
        plt.close()

def plot_stat2(data1, data2, log, saving, label):
    """ Plots the density functions of data1 and data 2 on the same figure

    If log is true, the scaling will be logarithmic.
    If saving countains the name of a file, the resulting figure will be saved in this file

    """

    plt.figure()

    plot_pdf_int(data1,"bo")                #fonctions de distribution
    plot_pdf_int(data2,"ro")
    plt.xlabel(label)
    plt.ylabel('Probability')
    if log:
        plt.xscale('log')
        plt.yscale('log')

    if saving:
        f=open(saving,'w')
        plt.savefig(f,format="pdf")
        f.close()
    else:
        plt.show()
        plt.close()

def plot_correlation(data1, data2, log, saving, xlabel, ylabel):

    plt.figure()

    plt.plot(data1,data2,'bo')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if log :
        plt.xscale('log')
        plt.yscale('log')

    if saving:
        f = open(saving,'w')
        plt.savefig(f, format = 'pdf')
        f.close()
    else:
        plt.show()
        plt.close()

def plot_dispersion(data1, data2, x_average, y_average, log, saving, xlabel, ylabel):

    plt.figure()

    plt.plot(data1,data2,'ro',markersize = 3.0,alpha = 0.1)
    plt.plot(x_average,y_average,'k-', linewidth = 0.5)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if log:
        plt.xscale('log')
        plt.yscale('log')

    if saving:
        f = open(saving,'w')
        plt.savefig(f,format = 'pdf')
        f.close()
    else:
        plt.show()
        plt.close()


def get_id(id_field):
    """ Returns as a string the id the id_field contains """
    if isinstance(id_field,int):
        fid = str(id_field)
    else:
        fid = str(id_field["$numberLong"])
    return fid

friends_error = ["Unauthorized","Not Found","Client Error (429)","Too Many Requests",
                 "unknown error","Service Unavailable","Authorization Required"]

############################################
#                                          #
#           SPECIAL : Graph Class          #
#                                          #
############################################

class Graph():
    """ Directed graph with in and out adjacency lists and a reciprocation method"""

    def __init__(self,weighted=True):
        self.edges = {}
        self.weighted = weighted

    def vertices(self):
        return self.edges.keys()

    def out_isolated_vertices(self, vertex_list):
        isolated = []
        for u in vertex_list:
            if u in self.edges:
                if self.out_degree(u) == 0:
                    isolated.append(u)
        return isolated

    def out_neighborhood(self,u):
       return self.edges[u]['out'].keys()

    def in_neighborhood(self,u):
        return self.edges[u]['in'].keys()

    def add_vertex(self,u):
        if not(u in self.edges):
            self.edges[u] = {'in':{},    # edges coming to u
                             'out':{}    # edges leaving from u
                             }

    def suppr_vertex(self,u):
        if u in self.edges:
            # remove edges to u
            for v in self.edges[u]['in']:
                del self.edges[v]['out'][u]
            # remove edges from u
            for v in self.edges[u]['out']:
                del self.edges[v]['in'][u]
            # remove u
            del self.edges[u]

    def add_edge(self,u,v):
        self.add_vertex(u)
        self.add_vertex(v)
        if v in self.edges[u]['out'] and self.weighted :
            # NB : in that case, necessarily, v is in self.edges[v]['in']
            self.edges[u]['out'][v] += 1
            self.edges[v]['in'][u] += 1
        else :
            self.edges[u]['out'][v] = 1
            self.edges[v]['in'][u] = 1

    def suppr_edge(self,u,v):
        if v in self.edges[u]['out']:
            # NB : in that case, necessarily, v in in self.edges[u]['in']
            del self.edges[u]['out'][v]
            del self.edges[v]['in'][u]

    def in_degree(self,u):
        return len(self.edges[u]['in'])

    def in_degree_list(self, vertex_list = [], withid = False):
        indegree = []
        for u in vertex_list:
            if u in self.edges:
                if withid:
                    indegree.append((u,self.in_degree(u)))
                else:
                    indegree.append(self.in_degree(u))
        return indegree

    def out_degree(self,u):
        return len(self.edges[u]['out'])

    def out_degree_list(self, vertex_list = [], withid = False):
        outdegree = []
        for u in vertex_list:
            if u in self.edges:
                if withid:
                    outdegree.append((u,self.out_degree(u)))
                else:
                    outdegree.append(self.out_degree(u))
        return outdegree

    def in_out_degree(self,u):
        return str(self.in_degree(u)) + " " + str(self.out_degree(u))

    def in_out_degree_list(self, vertex_list = [], withid = False):
        in_out_degree = []
        for u in vertex_list:
            if u in self.edges:
                if withid:
                    in_out_degree.append((u,self.in_out_degree(u)))
                else:
                    in_out_degree.append(self.in_out_degree(u))
        return in_out_degree

    def degree(self,u):
        """ Be careful : degree is here defined as outdegree + indegree """
        return self.in_degree(u) + self.out_degree(u)

    def degree_list(self, vertex_list =  [], withid=False):
        degree = []
        for u in vertex_list:
            if u in self.edges:
                if withid:
                    degree.append((u,self.degree(u)))
                else:
                    degree.append(self.degree(u))
        return degree

    def weight_list(self, vertex_list = [], withid=False):
        weight = []
        for u in vertex_list:
            if u in self.edges:
                for v in self.edges[u]['out']:
                    if withid:
                        weight.append((self.edges[u]['out'][v],u,v))
                    else:
                        weight.append(self.edges[u]['out'][v])
        return weight

    def in_strength(self,u):
        return sum(self.edges[u]['in'].values())

    def out_strength(self,u):
        return sum(self.edges[u]['out'].values())

    def strength(self,u):
        return (self.in_strength(u) + self.out_strength(u))

    def strength_list(self, vertex_list = [], withid=False):
        strength = []
        for u in vertex_list:
            if u in self.edges:
                if withid:
                    strength.append((self.strength(u),u))
                else:
                    strength.append(self.strength(u))
        return strength

    def reciprocation(self):
        """u - v iff u -> v et v -> u. Then, w(u - v) = w(u -> v) + w(v -> u)"""
        # filtering pure edges
        for u in self.edges:
            non_pure_edges = {}
            for v in self.edges[u]['out']:
                if v in self.edges[u]['in']:
                    non_pure_edges[v] = self.edges[u]['in'][v] + self.edges[u]['out'][v]
            self.edges[u]['in'] = dict(non_pure_edges)
            self.edges[u]['out'] = dict(non_pure_edges)

        # removing null degree vertices
        for u in self.edges.keys():
            if len(self.edges[u]['in']) == 0 and len(self.edges[u]['out']) == 0:
                del self.edges[u]

    def toNetworkX(self):
        G = nx.DiGraph()
        for u in self.edges:
            for v in self.edges[u]['out']:
                G.add_edge(u,v,weight=self.edges[u]['out'][v])
        return G

    def print_graph(self):
        for u in self.edges:
            for v in self.edges[u]['out']:
                print u," -> ",v," : ",self.edges[u]['out'][v]

    def print_edgelist(self, directed = True):
        for u in self.edges:
            for v in self.edges[u]['out']:
                if directed or (u <= v):
                    if self.weighted:
                        print u, v, self.edges[u]['out'][v]
                    else:
                        print u, v

###############################################################################################

def mentions_graph_from_tweets_file(file):
    """ Returns the graph of friends of the tweeter's users file """
    G = Graph()
    users = []

    for line in file:
        tweet = json.loads(line)
        uid = get_id(tweet['interaction']['author']['id'])
        users.append(uid)
        mention_ids = tweet['interaction'].get('mention_ids',[])
        for mention_id in mention_ids:
            vid = get_id(mention_id)
            G.add_edge(uid,vid)

    return (G,users)

def friends_graph_from_users_file(file):
    """ Returns the graph of friends of the tweeter's users file """
    G = Graph(weighted=False)
    users = []
    i=0
    t=datetime.datetime.now()
    for line in file:
        user = json.loads(line)
        uid = get_id(user["id"])
        friends = user.get("friends",[])
        if not(friends in friends_error):
            users.append(uid)
            for friend in friends:
                fid = get_id(friend)
                G.add_edge(uid,fid)
        if i%1000==0:
            print "%d: %s ms"%(i,str(datetime.datetime.now()-t))
            t=datetime.datetime.now()
        i+=1


    return (G,users)

graph_perso_conversion = {
    'friends' : friends_graph_from_users_file,
    'mentions' : mentions_graph_from_tweets_file}

def partition_from_file(file):
    partition = {}
    for line in file:
        [usrid, part] = line.split()
        if usrid in partition:
            raise Exception("personal_tools: partition_from_file: usrid already in partition")
        else:
            partition[usrid] = part

    return partition

def list_from_file(file):
    liste = []
    for line in file:
        liste.append(str(int(line)))

    return liste

def lexique_from_file(lexique):
    lex = {}
    for line in lexique:
        [ortho,phon,lemme,cgram,genre,nombre] = line.split(",")
        if ortho in lex:
            lex[ortho].append(lemme)
        else:
            lex[ortho] = [lemme]

    return lex

###############################################################################################
#                                                                                             #
#                                MATRIX OPERATIONS ON DICTIONARIES                            #
#                                                                                             #
###############################################################################################

def transposition(dict):
    dict2 = {}
    for col in dict:
        for lin in dict[col]:
            if lin in dict2:
                dict2[lin][col] = dict[col][lin]
            else:
                dict2[lin] = {col : dict[col][lin]}
    return dict2

def col_normalization(data):
    for col in data:
        n = float(sum(data[col].values()))
        for line in data[col]:
            data[col][line] = float(data[col][line]) / n
    return data

def lin_normalization(data):
    return transposition(col_normalization(transposition(data)))
