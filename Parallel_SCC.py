# coding: utf-8

# In[14]:
from pyspark import SparkConf, SparkContext
import networkx as nx
import matplotlib.pyplot as plt
import itertools
from itertools import chain
import numpy as np
import random
import time

#G = nx.DiGraph()
#G.add_edges_from([(1, 0), (0, 4), (2, 1), (4, 1), (5, 4), (5, 1), (5, 6), (6, 5), (2, 3), (3, 2), (7, 7), (7, 3), (7, 6), (6,2)])
#G = nx.gn_graph(1500)
#random.seed(1)
G=nx.read_edgelist("p2p-Gnutella_10000.txt",nodetype=int)
#pos = nx.spring_layout(G)
#nx.draw_networkx_nodes(G, pos, node_size = 500)
#nx.draw_networkx_labels(G, pos)
#nx.draw_networkx_edges(G, pos,arrows=True)
#plt.axis('off')
#plt.show()
#print(nx.to_dict_of_lists(G, nodelist=None))

V = list(G.nodes())
sc_conf = SparkConf()
sc_conf.setAppName("SCC_Graph")
sc_conf.setMaster('local[1]')
sc_conf.set('spark.executor.memory', '4g')
sc_conf.set('spark.executor.cores', '10')
sc_conf.set('spark.cores.max', '40')
sc_conf.set('spark.logConf', True)
print(sc_conf.getAll())

sc = None
try:
	sc.stop()
	sc = SparkContext(conf=sc_conf)
except:
	sc = SparkContext(conf=sc_conf)

#SparkConf.set("spark.executor.instances", "20")
def Desc(G,v):
    nodes = list(G.nodes())
    Desc_v = list()
    for a in nodes:
        try:
            temp = list(nx.all_shortest_paths(G, source=v, target=a))
            temp = list(chain.from_iterable(temp))
            if temp:
                Desc_v.append(temp[-1])
        except nx.exception.NetworkXNoPath:
            continue
    return Desc_v

def Pred(G,v):
    nodes = list(G.nodes())
    Pred_v = list()
    for a in nodes:
        try:
            temp = list(nx.all_shortest_paths(G, source=a, target=v))
            temp = list(chain.from_iterable(temp))
            if temp:
                Pred_v.append(temp[0])
        except nx.exception.NetworkXNoPath:
            continue
    return Pred_v
TotalSCC = list()
def DCSC(V):
    SCC = list()
    if V:
        v = random.choice(V)
        Pred_v = Pred(G,v)
        Desc_v = Desc(G,v)

        elements = list()
        dict1 = {v:Pred_v}
        dict2 = {v:Desc_v}
        elements.append(dict1)
        elements.append(dict2)
        RDD = sc.parallelize(elements)
        
        totalLength = RDD.reduce(lambda a, b: (np.intersect1d(list(chain.from_iterable(list(a.values()))),(list(chain.from_iterable(b.values()))))).tolist())
        SCC = totalLength
        print(SCC)
        #SCC = (np.intersect1d(Pred(G,v), Desc(G,v))).tolist()
        if SCC not in TotalSCC:
            TotalSCC.append(SCC)
            
        Rem_v = list(set(V).difference(set(np.unique(np.concatenate((Desc_v,Pred_v),0)))))
        
        V = list(set(V).difference(set(list(chain.from_iterable(TotalSCC)))))

        DCSC(list(set(Pred_v).difference(set((list(chain.from_iterable(TotalSCC)))))))
        DCSC(list(set(Desc_v).difference(set((list(chain.from_iterable(TotalSCC)))))))
        DCSC(Rem_v)
    else:
        return
start_time = time.time()
DCSC(V)
print("Strongly connected components in this graph are: ",TotalSCC)
print("Strongly connected components in this graph are: ",len(TotalSCC))
print("--- %s seconds ---" % round((time.time() - start_time)))

