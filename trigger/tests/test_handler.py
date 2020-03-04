"""
Tests for the AWS Lambda handler.

"""
import datetime
import json
from unittest import TestCase, mock

from ..handler import lambda_handler, serialize_datetime


class TestSerializeDatetime(TestCase):

    def setUp(self):
        self.dt = datetime.datetime(2020, 1, 1, 16, 47, 10)

    def test_datetime_serialization(self):
        result = serialize_datetime(self.dt)
        self.assertEqual(result, self.dt.isoformat())


class TestLambdaHandler(TestCase):

    state_machine_arn = 'arn:aws:states:us-south-19:389051134:stateMachine:MyStateMachine'
    region = 'us-south-19'

    def setUp(self):
        self.s3_event = {
            'Records': [{'eventSource': 'aws:s3'}]
        }
        self.s3_event_two = {
            'Records': [{'eventSource': 'aws:s3'}, {'eventSource': 'aws:s3'}]
        }
        self.sqs_event = {
            'Records': [{'eventSource': 'aws:sqs', 'body': '{"some": "garbage"}'}]
        }
        self.other_event = {
            'Records': [{'eventSource': 'aws:blah'}]
        }
        self.context = {'context': '???'}

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region})
    @mock.patch('trigger.handler.execute_state_machine')
    def test_single_s3_record(self, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 1, 1, 19, 25, 40)}
        result = lambda_handler(self.s3_event, self.context)
        expected_result = {'Responses': [{'spam': 'eggs', 'startDate': '2020-01-01T19:25:40'}]}
        mock_esm.assert_called_with(
            state_machine_arn=self.state_machine_arn,
            invocation_payload=json.dumps({'Record': self.s3_event['Records'][0], 'resumeState': None}),
            region=self.region
        )
        self.assertDictEqual(result, expected_result)

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region})
    @mock.patch('trigger.handler.execute_state_machine')
    def test_two_s3_records(self, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 1, 1, 19, 25, 50)}
        lambda_handler(self.s3_event_two, self.context)
        self.assertEqual(mock_esm.call_count, 2)

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region})
    @mock.patch('trigger.handler.execute_state_machine')
    def test_sqs_record(self, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 1, 1, 19, 31, 21)}
        lambda_handler(self.sqs_event, self.context)
        mock_esm.assert_called_with(
            state_machine_arn=self.state_machine_arn,
            invocation_payload='{"some": "garbage"}',
            region=self.region
        )

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region})
    def test_unsupported_source(self):
        with self.assertRaises(TypeError):
            lambda_handler(self.other_event, self.context)
