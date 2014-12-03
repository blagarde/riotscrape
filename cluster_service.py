from cluster.preprocessing.std_scaler import StdScaler
from cluster.labelling.sk_labelling import SkLabelling
from cluster.translator.ritu_translator import RituTranslator

class ClusterService(object):
    
    def __init__(self, scaler_fn, clf_fn):
        self.translator = RituTranslator()
        self.scaler = StdScaler().load(scaler_fn)
        self.clf = SkLabelling(clf_file=clf_fn)

    def get_user_cluster(self, user):
        u_v = self.translator.translate(user)
        print u_v
        if len(u_v) > 0:
            u_s = self.scaler.transform(u_v)
            print u_s
            label = self.clf.get_label(u_s)
            print label
            return label
        else:
            print "Error: unable to translate user"
            return -1
