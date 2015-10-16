import qgis


def showUsers(community):
    print "show users in community %s"%community
    layers = qgis.utils.iface.legendInterface().layers()
    for layer in layers:
        if layer.name()==u'usersCentroids':
            userLayer = layer
    userLayer.setSubsetString(u'"community"=%s'%community)

print "community [% "id" %]"
print "[% "nbSpeakers" %] speakers"
print "[% "nPoints" %] points"
print "average distance to centroid : [% "averageDistance" %]"

showUsers([% "id" %])
