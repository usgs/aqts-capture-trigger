"""
Functions for triggering an AWS Step Function.

"""
import boto3


def execute_state_machine(state_machine_arn, invocation_payload, region='us-west-2'):
    """
    Kicks off execution of a state machine in AWS Step Functions.

    :param str state_machine_arn: Amazon Resource Number (ARN) for the step function to be triggered
    :param str invocation_payload: JSON data to kick off the step function
    :param str region: AWS region where the step function is deployed

    """
    sf = boto3.client('stepfunctions', region_name=region)
    resp = sf.start_execution(
        stateMachineArn=state_machine_arn,
        input=invocation_payload
    )
    return resp
