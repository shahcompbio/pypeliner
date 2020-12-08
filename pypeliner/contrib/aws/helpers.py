import logging
import random
import string


class AwsLoggingFilter(logging.Filter):
    """
    filter out AWS logging output from pypeliner log
    """
    def __init__(self):
        self.filter_keywords = [
            'botocore', 'boto3', 'urllib3', 's3transfer'
        ]

    def filter(self, rec):
        """
        filter a log record or not
        :param rec: log record
        :type rec:
        :return: True if filtered else False
        :rtype: bool
        """
        logname = rec.name.split('.')[0]
        if logname in self.filter_keywords:
            return False
        return True


def set_aws_logging_filters():
    """
    add aws filter to root logging handlers
    """
    for handler in logging.root.handlers:
        handler.addFilter(AwsLoggingFilter())


def random_string(length):
    """
    generate a random string of length
    :param length: length of string
    :type length: int
    :return: randomly generated string
    :rtype: str
    """
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))
