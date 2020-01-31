"""
Tests for the AWS Lambda handler

"""
import json
from unittest import TestCase, mock

from ..handler import lambda_handler


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
    def test_lambda_handler(self, m_client):
        lambda_handler(self.event, self.context)
        m_client.assert_called_with(
            state_machine_arn=self.state_machine_arn,
            invocation_payload=json.dumps(self.event),
            region=self.region
        )
