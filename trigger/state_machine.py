"""
Functions for triggering an AWS Step Function.

"""
import boto3


class AwsStepFunction:
    """AWS Step Function object with methods to act upon
    a particular step function.

    :param str state_machine_arn: AWS generated state machine ARN that uniquely identifies the state machine
    :param str region: AWS region where the step function is deployed
    """

    def __init__(self, state_machine_arn, region='us-west-2'):
        self.arn = state_machine_arn
        self.sf = boto3.client('stepfunctions', region_name=region)

    def start_execution(self, invocation_payload):
        """Kicks off execution of a state machine in AWS Step Functions.

        :param str invocation_payload: JSON data to kick off the step function
        """
        resp = self.sf.start_execution(
            stateMachineArn=self.arn,
            input=invocation_payload
        )
        return resp
