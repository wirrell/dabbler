"""
Specific errors for dabbler.
"""


class NoWeatherInformationError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SimulationFailedError(Exception):
    def __init__(self, message):
        super().__init__(message)
