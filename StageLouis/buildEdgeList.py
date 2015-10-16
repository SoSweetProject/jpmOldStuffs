import personal_tools
import logging
import networkx
import snap

logger = logging.getLogger(__name__)
handler = logging.FileHandler('logger.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

logger.info("building graph")
(G,users) = personal_tools.friends_graph_from_users_file(open('users.json'))

logger.info("converting graph")
gnx=G.toNetworkX()

logger.info("writing graph")
networkx.write_edgelist(gnx,"net.txt")

logger.info("done.")


def reciproqueSnapGraph(Gin):
    Gout = snap.TUNGraph.New()
    for edge in Gin.Edges():
        if Gin.IsEdge(edge.GetDstNId(), edge.GetSrcNId()):
            if not Gout.IsNode(edge.GetSrcNId()):
                Gout.AddNode(edge.GetSrcNId())
            if not Gout.IsNode(edge.GetDstNId()):
                Gout.AddNode(edge.GetDstNId())
            Gout.AddEdge(edge.GetSrcNId(), edge.GetDstNId())
    return Gout
