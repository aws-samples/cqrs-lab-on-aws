# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import unittest

from aws_cdk import core

from cqrs.cqrs_construct import CQRSConstruct

class TestCQRSConstruct(unittest.TestCase):

    def setUp(self):
        self.app = core.App()
        self.stack = core.Stack(self.app, "TestStack")
    
    def test_num_buckets(self):
        num_buckets = 10
        cqrs = CQRSConstruct(self.stack, "Test1", num_buckets)
        assert len(cqrs.buckets) == num_buckets