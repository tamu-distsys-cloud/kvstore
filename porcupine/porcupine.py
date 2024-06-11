import time
from typing import List, Tuple, Any

from porcupine.model import Operation, Model, Event
from porcupine import checker

def check_operations(model: Model, history: List[Operation]) -> bool:
    res, _ = checker.check_operations(model, history, False, 0)
    return res == CheckResult.Ok

# timeout = 0 means no timeout
# if this operation times out, then a false positive is possible
def check_operations_timeout(model: Model, history: List[Operation], timeout: float) -> str:
    res, _ = checker.check_operations(model, history, False, timeout)
    return res

# timeout = 0 means no timeout
# if this operation times out, then a false positive is possible
def check_operations_verbose(model: Model, history: List[Operation], timeout: float) -> Tuple[str, checker.LinearizationInfo]:
    return checker.check_operations(model, history, True, timeout)

def check_events(model: Model, history: List[Event]) -> bool:
    res, _ = checker.check_events(model, history, False, 0)
    return res == "Ok"

# timeout = 0 means no timeout
# if this operation times out, then a false positive is possible
def check_events_timeout(model: Model, history: List[Event], timeout: float) -> str:
    res, _ = checker.check_events(model, history, False, timeout)
    return res

# timeout = 0 means no timeout
# if this operation times out, then a false positive is possible
def check_events_verbose(model: Model, history: List[Event], timeout: float) -> Tuple[str, checker.LinearizationInfo]:
    return checker.check_events(model, history, True, timeout)
