import unittest
import os
from user_test import user, hl_keys
from cluster_service import ClusterService
from learner.utils.path_utils import local_dir

class ClusterServiceTest(unittest.TestCase):
    
    def setUp(self):
        self.cs = ClusterService(scaler_fn=os.path.join(local_dir(__file__), 'scaler.pkl'), clf_fn=os.path.join(local_dir(__file__),'clf.pkl'))
        self.cs.translator.features = hl_keys

    def test_cluster_service(self):
        result = self.cs.get_user_cluster(user)
        self.assertEqual(result, 2)
