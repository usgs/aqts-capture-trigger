"""
Tests for the AWS Lambda handler.

"""
import datetime
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
        self.sqs_event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'body': '{"Records": [{"eventSource": "s3", "eventTime": "2020-02-17T12:39Z", "s3": {"object": {"size": 618}}}]}'
                }
            ]
        }
        self.sqs_giant_event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'body': '{"Records": [{"eventSource": "s3", "eventTime": "2020-02-17T12:39Z", "s3": {"object": {"key": "foo", "size": 618000000000}}}]}'
                }
            ]
        }

        self.sqs_event_two = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'body': '{"Records": [{"eventSource": "s3", "eventTime": "2020-02-17T12:39Z", "s3": {"object": {"size": 618}}}, {"eventSource": "s3", "eventTime": "2020-02-17T12:40Z", "s3": {"object": {"size": 618}}}]}'
                },
                {
                    'eventSource': 'aws:sqs',
                    'body': '{"Records": [{"eventSource": "s3", "eventTime": "2020-02-18T21:59Z", "s3": {"object": {"size": 618}}}, {"eventSource": "s3", "eventTime": "2020-02-18T22:00Z", "s3": {"object": {"size": 618}}}]}'
                }
            ]
        }
        self.sqs_error_event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'body': '{"Record": {"eventSource": "s3", "eventTime": "2020-02-18T21:59Z", "s3": {"object": {"size": 618}}}, "stepFunctionFails": 1}',
                    'attributes': {'MessageGroupId': 'step_function_error'}
                },
                {
                    'eventSource': 'aws:sqs',
                    'body': '{"id": 74205, "type": "correctedData", "stepFunctionFails": 1}',
                    'attributes': {'MessageGroupId': 'step_function_error'}
                }
            ]
        }
        self.sqs_error_event_initial_client_error = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'body': '{"Record": {"eventSource": "s3", "eventTime": "2020-02-18T21:59Z", "s3": {"object": {"size": 618}}}}',
                    'attributes': {'MessageGroupId': 'step_function_error'}
                }
            ]
        }
        self.other_event = {
            'Records': [{'eventSource': 'aws:blah'}]
        }
        self.context = {'context': '???'}

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region, 'OBJECT_SIZE_LIMIT': str(10**7)})
    @mock.patch('trigger.handler.execute_state_machine', autospec=True)
    def test_sqs_record(self, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 1, 1, 19, 31, 21)}
        lambda_handler(self.sqs_event, self.context)
        mock_esm.assert_called_with(
            state_machine_arn=self.state_machine_arn,
            invocation_payload='{"Record": {"eventSource": "s3", "eventTime": "2020-02-17T12:39Z", "s3": {"object": {"size": 618}}}}',
            region=self.region
        )

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region, 'OBJECT_SIZE_LIMIT': str(10**7)})
    @mock.patch('trigger.handler.execute_state_machine', autospec=True)
    def test_two_sqs_records(self, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 2, 18, 22, 00, 41)}
        lambda_handler(self.sqs_event_two, self.context)
        self.assertEqual(mock_esm.call_count, 4)

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'STAGE': 'TEST',
                                    'AWS_DEPLOYMENT_REGION': region,
                                    'OBJECT_SIZE_LIMIT': str(150000000)})
    @mock.patch('trigger.handler.execute_state_machine', autospec=True)
    @mock.patch('trigger.handler.send_to_chopper', autospec=True)
    def test_giant_sqs_record(self, mock_stc, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 2, 18, 22, 00, 41)}
        mock_stc.return_value = None
        lambda_handler(self.sqs_giant_event, self.context)
        """
        Zero calls because the giant file has been thrown in the chopping queue
        """
        self.assertEqual(mock_esm.call_count, 0)

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region, 'OBJECT_SIZE_LIMIT': str(10**7)})
    @mock.patch('trigger.handler.execute_state_machine', autospec=True)
    @mock.patch('trigger.handler.send_to_chopper', autospec=True)
    def test_error_path(self, mock_stc, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 2, 18, 22, 1, 9)}
        mock_stc.return_value = None
        lambda_handler(self.sqs_error_event, self.context)
        call_0 = mock.call(
            state_machine_arn='arn:aws:states:us-south-19:389051134:stateMachine:MyStateMachine',
            invocation_payload='{"Record": {"eventSource": "s3", "eventTime": "2020-02-18T21:59Z", "s3": {"object": {"size": 618}}}, "stepFunctionFails": 1}',
            region='us-south-19'
        )
        call_1 = mock.call(
            state_machine_arn='arn:aws:states:us-south-19:389051134:stateMachine:MyStateMachine',
            invocation_payload='{"id": 74205, "type": "correctedData", "stepFunctionFails": 1}',
            region='us-south-19'
        )
        calls = [call_0, call_1]
        mock_esm.assert_has_calls(calls, any_order=True)

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region, 'OBJECT_SIZE_LIMIT': str(10**7)})
    @mock.patch('trigger.handler.execute_state_machine', autospec=True)
    def test_error_where_input_is_the_original(self, mock_esm):
        mock_esm.return_value = {'spam': 'eggs', 'startDate': datetime.datetime(2020, 2, 18, 22, 1, 9)}
        lambda_handler(self.sqs_error_event_initial_client_error, self.context)
        mock_esm.assert_called_with(
            state_machine_arn=self.state_machine_arn,
            invocation_payload='{"Record": {"eventSource": "s3", "eventTime": "2020-02-18T21:59Z", "s3": {"object": {"size": 618}}}}',
            region=self.region
        )

    @mock.patch.dict('os.environ', {'STATE_MACHINE_ARN': state_machine_arn, 'AWS_DEPLOYMENT_REGION': region, 'OBJECT_SIZE_LIMIT': str(10**7)})
    def test_unsupported_source(self):
        with self.assertRaises(TypeError):
            lambda_handler(self.other_event, self.context)
