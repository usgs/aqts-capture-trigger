"""
Tests for the AWS Lambda handler

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
        self.event = {
            'Records': [{'eventSource': 's3'}]
        },
        self.context = {'context': '???'}

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region})
    @mock.patch('trigger.handler.execute_state_machine')
    def test_lambda_handler(self, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 1, 1, 19, 25, 40)}
        result = lambda_handler(self.event, self.context)
        expected_result = {'spam': 'eggs', 'startDate': '2020-01-01T19:25:40'}
        mock_esm.assert_called_with(
            state_machine_arn=self.state_machine_arn,
            invocation_payload=json.dumps(self.event),
            region=self.region
        )
        self.assertEqual(result, expected_result)
