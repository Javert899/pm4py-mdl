class ObjCentricMultigraph(object):
    def __init__(self, dictio):
        self.dictio = dictio

    def __repr__(self):
        ret = []
        ret.append("\nObject-Centric Multigraph\n\n")
        ret.append("Activities (Events, Objects, EO):\n")
        for act in self.dictio["activities"]:
            ret.append("\t%s\t%d\t%d\t%d\n" % (
                act, self.dictio["activities"][act]["events"], self.dictio["activities"][act]["objects"],
                self.dictio["activities"][act]["eo"]))
        ret.append("\nTypes:\n")
        for tk in self.dictio["types_view"]:
            ret.append("\t%s\n" % (tk))
            t = self.dictio["types_view"][tk]
            ret.append("\t\tstart activities: ")
            for act in t["start_activities"]:
                ret.append("(%s,%d,%d,%d) " % (
                act, t["start_activities"][act]["events"], t["start_activities"][act]["objects"],
                t["start_activities"][act]["eo"]))
            ret.append("\n\t\tend activities: ")
            for act in t["end_activities"]:
                ret.append("(%s,%d,%d,%d) " % (
                act, t["end_activities"][act]["events"], t["end_activities"][act]["objects"],
                t["end_activities"][act]["eo"]))
            ret.append("\n")
        return "".join(ret)
