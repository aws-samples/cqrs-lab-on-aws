# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_rds as rds,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_dynamodb as ddb,
    aws_kinesis as kinesis,
    aws_apigateway as apigw,
    aws_lambda_event_sources as event_sources,
    core
)


class CQRSStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self._table = ddb.Table(
            self, 'AirTicketOrder',
            partition_key={'name': 'customer_id',
                           'type': ddb.AttributeType.STRING},
            stream=ddb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        self.lambda_cmd = _lambda.Function(
            self, 'CommandDDBSaver',
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.asset("./lambda/cmd/"),
            handler="cmd.lambda_handler",
            environment={
                "ORDER_TABLE_NAME": self._table.table_name,
            }
        )

        self._table.grant_read_write_data(self.lambda_cmd)

        # Allow Command lambda to invoke other lambda
        self.lambda_cmd.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=["lambda:InvokeFunction"]
                )
            )

        api = apigw.LambdaRestApi(
            self,
            "CommandEndPoint",
            handler=self.lambda_cmd,
        )

        # TODO: 因为2个AZ，可以只生命一个公网和一个私网，这样X2 AZ就会2 pub + 2 pri
        # Lambda access RDS Aurora MySQL requires VPC for security and perf
        vpc = ec2.Vpc(self, 'air-ticket',
                      cidr="10.125.0.0/16",
                      max_azs=2,
                      nat_gateways=1,
                      subnet_configuration=[
                          ec2.SubnetConfiguration(
                            name="public1", cidr_mask=24, subnet_type=ec2.SubnetType.PUBLIC),
                          ec2.SubnetConfiguration(
                              name="public2", cidr_mask=24, subnet_type=ec2.SubnetType.PUBLIC),
                          ec2.SubnetConfiguration(
                              name="private1", cidr_mask=24, subnet_type=ec2.SubnetType.PRIVATE),
                          ec2.SubnetConfiguration(
                              name="private2", cidr_mask=24, subnet_type=ec2.SubnetType.PRIVATE)
                      ]
                      )

        query_lambda_sg = ec2.SecurityGroup(
            self, 'Query-Lambda-SG',
            vpc=vpc,
            description="Allows DB connections from Query Lambda SG",
        )

        sink_lambda_sg = ec2.SecurityGroup(
            self, 'RDS-Sink-Lambda-SG',
            vpc=vpc,
            description="Allows DB connections from Sink Lambda SG",
        )

        db_name = "Demo"
        db_user_name = 'admin'
        db_user_passowrd = 'password'

        parameter_group = rds.ParameterGroup(self, "ParameterGroup",
                                             family="mysql5.7",
                                             parameters={
                                             })
        aurora_db = rds.DatabaseInstance(
            self, "air-ticket-db",
            master_user_password=core.SecretValue.ssm_secure(
                'AirTicket.AdminPass', version='1'),
            master_username=db_user_name,
            engine=rds.DatabaseInstanceEngine.MYSQL,
            engine_version="5.7",
            parameter_group=parameter_group,
            vpc=vpc,
            # Disable deletion protection for auto deletion
            deletion_protection=False,
            instance_class=ec2.InstanceType.of(
                ec2.InstanceClass.MEMORY5, ec2.InstanceSize.XLARGE),
            removal_policy=core.RemovalPolicy.DESTROY
        )

        self._query_handler = _lambda.Function(
            self, 'QueryHandler',
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.asset("./lambda/query/"),
            handler="query.lambda_handler",
            timeout=core.Duration.seconds(60),
            vpc=vpc,
            security_group=query_lambda_sg,
            environment={
                "AuroraEndpoint": aurora_db.db_instance_endpoint_address,
                "dbName": db_name,
                "dbPassword": db_user_passowrd,
                "dbUser": db_user_name
            }
        )

        query_api = apigw.LambdaRestApi(
            self, "Query",
            handler=self._query_handler,
        )

        # Init DB Lambda
        self.lambda_init = _lambda.Function(
            self, 'InitDBHandler',
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.asset("./lambda/initdb/"),
            handler="init.lambda_handler",
            timeout=core.Duration.seconds(60),
            vpc=vpc,
            security_group=query_lambda_sg,
            environment={
                "AuroraEndpoint": aurora_db.db_instance_endpoint_address,
                "dbName": db_name,
                "dbPassword": db_user_passowrd,
                "dbUser": db_user_name
            }
        )

        self.lambda_cmd.add_environment('INITDB_LAMBDA_NAME', self.lambda_init.function_name)

        # Create stream for fan-out
        stream_name='kinesis-stream-for-fanout'

        # Sync DDB stream delta to RDS Lambda
        self.lambda_sync = _lambda.Function(
            self, 'SyncHandler',
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.asset("./lambda/sync/"),
            handler="sync.lambda_handler",
            timeout=core.Duration.seconds(60),
            vpc=vpc,
            security_group=query_lambda_sg,
            environment={
                "streamName": stream_name
            }
        )

        # Add DDB stream trigger to sync lambda
        self.lambda_sync.add_event_source(
            event_sources.DynamoEventSource(self._table,
                starting_position=_lambda.StartingPosition.TRIM_HORIZON) )

        self._table.grant_stream_read(self.lambda_sync)

        # Allow init/sync lambda access MySQL
        aurora_db.connections.allow_from(
            query_lambda_sg, ec2.Port.tcp(3306),
            "Allow MySQL access from Query Lambda (because Aurora actually exposes PostgreSQL/MySQL on port 3306)",
        )

        aurora_db.connections.allow_from(
            sink_lambda_sg, ec2.Port.tcp(3306),
            "Allow MySQL access from Sink Lambda (because Aurora actually exposes PostgreSQL/MySQL on port 3306)",
        )


        strm = kinesis.Stream(self, 'kinesis-stream-for-fanout', stream_name=stream_name)

        # Create RDS Sink Lambda
        self.lambda_rds_sink = _lambda.Function(
            self, 'RDS_SINK_1',
            handler='rds_sinker.lambda_handler',
            code=_lambda.Code.asset("./lambda/sink/"),
            runtime=_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.seconds(300),
            vpc=vpc,
            security_group=sink_lambda_sg,
            environment={
                "AuroraEndpoint": aurora_db.db_instance_endpoint_address,
                "dbName": db_name,
                "dbPassword": db_user_passowrd,
                "dbUser": db_user_name
            }
        )

        # Update Lambda Permissions To Use Stream
        strm.grant_read_write(self.lambda_sync)
        strm.grant_read(self.lambda_rds_sink)

        stream_consumer = kinesis.CfnStreamConsumer(self,
            'lambda-efo-consumer-id',
            consumer_name='lambda-efo-consumer',
            stream_arn=strm.stream_arn)

        e_s_mappnig = _lambda.EventSourceMapping(
            self,
            'lambda-efo-consumer-event-source-mapping',
            target=self.lambda_rds_sink,
            event_source_arn=stream_consumer.stream_arn,
            batch_size=1,
            starting_position=_lambda.StartingPosition.TRIM_HORIZON,
        )

        # self.lambda_rds_sink.add_event_source_mapping(e_s_mappnig)

        # CDK below create lambda as a standand Kinesis consumer instead of EFO
        # 
        # # Create New Kinesis Event Source
        # kinesis_stream_event_source = event_sources.KinesisEventSource(
        #     stream=strm,
        #     starting_position=_lambda.StartingPosition.TRIM_HORIZON,
        #     batch_size=1
        # )

        # # Attach New Event Source To Lambda
        # self.lambda_rds_sink.add_event_source(kinesis_stream_event_source)


        # Create dead letter queue and grant send permission to sync/sink lambda
        self._queue = sqs.Queue(
            self, "DeadLetterQueue",

            #Amazon SQS sets a visibility timeout, a period of time during which Amazon 
            # SQS prevents other consumers from receiving and processing the message. 
            # The default visibility timeout for a message is 30 seconds. 
            # The minimum is 0 seconds. The maximum is 12 hours.
            visibility_timeout=core.Duration.seconds(300),
        )

        self._queue.grant_send_messages(self.lambda_sync)
        self._queue.grant_send_messages(self.lambda_rds_sink)

        self.lambda_sync.add_environment("DLQ_NAME", self._queue.queue_name)