"""
Tests for the state_machine module

"""
import json
from unittest import TestCase, mock

from ..state_machine import execute_state_machine


class TestExecuteStateMachine(TestCase):

    def setUp(self):
        self.state_machine_arn = 'arn:aws:states:us-south-19:389051134:stateMachine:MyStateMachine'
        self.invocation_payload = json.dumps({'spam': 'eggs'})
        self.region = 'us-south-19'

    @mock.patch('trigger.state_machine.boto3.client', autospec=True)
    def test_start_execution(self, m_client):
        execute_state_machine(
            self.state_machine_arn,
            self.invocation_payload,
            self.region
        )
        m_client.assert_called_with('stepfunctions', region_name=self.region)
        m_client.return_value.start_execution.assert_called_with(
            stateMachineArn=self.state_machine_arn,
            input=self.invocation_payload
        )
