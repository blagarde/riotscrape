from sklearn.preprocessing import StandardScaler
from learner.labelling.sk_labelling import SkLabelling
from learner.translator.riot_hl_translator import RiotHLTranslator
import pickle

class ClusterService(object):
    
    def __init__(self, scaler_fn='labelling_files/scaler.pkl', clf_fn='labelling_files/clf.pkl'):
        self.translator = RiotHLTranslator()
        self.scaler = self.load(scaler_fn)
        self.clf = SkLabelling(clf_file=clf_fn)

    def get_user_cluster(self, user):
        u_v = self.translator.transform(user)
        if len(u_v) > 0:
            u_s = self.scaler.transform(u_v)
            label = int(self.clf.get_label(u_s)[0])
            return label
        else:
            print "Error: unable to translate user"
            return -1
    
    def load(self, fn):
        with open(fn, 'rb') as f:
            return pickle.load(f)
