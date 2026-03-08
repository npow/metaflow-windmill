from metaflow.exception import MetaflowException


class WindmillException(MetaflowException):
    headline = "Windmill error"


class NotSupportedException(WindmillException):
    headline = "Windmill feature not supported"
