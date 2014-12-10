from sklearn.preprocessing import StandardScaler
from learner.labelling.sk_labelling import SkLabelling
from learner.translator.riot_hl_translator import RiotHLTranslator
import pickle

class ClusterService(object):
    '''
    This class aims to compute high level features from json user, 
    and then apply clustering pipeline to compute the corresponding label

    scaler_fn and clf_fn are the scaler pickle and the classifier pickle 
    written when clustering_pipe.write is called

    TODO we should be able to use a config file to load all steps of clustering_pipeline
    '''
    
    def __init__(self, scaler_fn='labelling_files/scaler.pkl', clf_fn='labelling_files/clf.pkl'):
        self.translator = RiotHLTranslator()
        self.scaler = self._load(scaler_fn)
        self.clf = SkLabelling(clf_file=clf_fn)

    def get_user_cluster(self, user):
        '''
        take a user as json (like the ones from ES)

        returns the computed label (int)
        if there is an error (unable to translate, or to label) returns -1
        '''
        u_v = self.translator.transform(user)
        if len(u_v) > 0:
            u_s = self.scaler.transform(u_v)
            label = int(self.clf.get_label(u_s)[0])
            return label
        else:
            print "Error: unable to translate user"
            return -1
    
    def _load(self, fn):
        with open(fn, 'rb') as f:
            return pickle.load(f)
