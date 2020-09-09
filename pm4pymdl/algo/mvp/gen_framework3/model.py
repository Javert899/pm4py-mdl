class ObjCentricMultigraph(object):
    def __init__(self, dictio):
        self.dictio = dictio

    def __repr__(self):
        ret = []
        ret.append("\nObject-Centric Multigraph\n\n")
        ret.append("Activities (Events, Objects, EO):\n")
        for act in self.dictio["activities"]:
            ret.append("\t%s\tE=%d\tO=%d\tEO=%d\n" % (
                act, self.dictio["activities"][act]["events"], self.dictio["activities"][act]["objects"],
                self.dictio["activities"][act]["eo"]))
        ret.append("\nTypes:\n")
        for tk in self.dictio["types_view"]:
            ret.append("\t%s\n" % (tk))
            t = self.dictio["types_view"][tk]
            ret.append("\t\tstart activities: ")
            for act in t["start_activities"]:
                ret.append("(%s,E=%d,O=%d,EO=%d,min_obj=%d,max_obj=%d,must=%s) " % (
                    act, t["start_activities"][act]["events"], t["start_activities"][act]["objects"],
                    t["start_activities"][act]["eo"], t["start_activities"][act]["min_obj"],
                    t["start_activities"][act]["max_obj"], t["start_activities"][act]["must"]))
            ret.append("\n\t\tend activities: ")
            for act in t["end_activities"]:
                ret.append("(%s,E=%d,O=%d,EO=%d,min_obj=%d,max_obj=%d,must=%s) " % (
                    act, t["end_activities"][act]["events"], t["end_activities"][act]["objects"],
                    t["end_activities"][act]["eo"], t["end_activities"][act]["min_obj"],
                    t["end_activities"][act]["max_obj"], t["end_activities"][act]["must"]))
            ret.append("\n\t\tedges: \n")
            for edge in t["edges"]:
                ret.append("\t\t\t%s->%s E=%d O=%d EO=%d min_obj=%d max_obj=%d must=%s\n" % (
                    edge[0], edge[1], t["edges"][edge]["events"], t["edges"][edge]["objects"], t["edges"][edge]["eo"],
                    t["edges"][edge]["min_obj"], t["edges"][edge]["max_obj"], t["edges"][edge]["must"]))
        ret.append("\nMapping:\n")
        for act in self.dictio["activities_mapping"]:
            ret.append("\t%s\t%s\n" % (act, self.dictio["activities_mapping"][act]))
        return "".join(ret)
