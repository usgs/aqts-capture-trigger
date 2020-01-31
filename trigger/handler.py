import json
import logging
import os


from .state_machine import execute_state_machine

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    logger.info(f'Event: {event}')
    logger.info(f'Context: {context}')

    state_machine_arn = os.getenv('STATE_MACHINE_ARN')
    region = os.getenv('AWS_DEPLOYMENT_REGION')

    payload = json.dumps(event)
    resp = execute_state_machine(
        state_machine_arn=state_machine_arn,
        invocation_payload=payload,
        region=region
    )
    logger.info(f'State Machine Response: {resp}')
    return resp
