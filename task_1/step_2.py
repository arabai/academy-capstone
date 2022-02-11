import boto3
import pprint


def get_secrets():
    client = boto3.client('secretsmanager')

    response = client.get_secret_value(
        SecretId='snowflake/capstone/login'
    )

    return response


if __name__ == '__main__':
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(get_secrets())