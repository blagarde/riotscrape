import unittest
from user_test import user, hl_keys
from cluster_service import ClusterService

class ClusterServiceTest(unittest.TestCase):
    
    def setUp(self):
        self.cs = ClusterService(scaler_fn='test/scaler.pkl', clf_fn='test/clf.pkl')
        self.cs.translator.features = hl_keys

    def test_cluster_service(self):
        result = self.cs.get_user_cluster(user)
        self.assertEqual(result, 2)
