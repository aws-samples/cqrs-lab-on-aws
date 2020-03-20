# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

#!/usr/bin/env python3

from aws_cdk import core

from cqrs.cqrs_stack import CQRSStack


app = core.App()

CQRSStack(app, "cqrs", env={'region': 'us-east-1'})

app.synth()
