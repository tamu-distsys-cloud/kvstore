from typing import Any, Callable, List, Tuple

class Operation:
    def __init__(self, client_id: int, input: Any, call_time: int, output: Any, response_time: int):
        # Optional, unless you want a visualization; zero-indexed
        self.client_id = client_id
        self.input = input
        self.call_time = call_time # invocation time
        self.output = output
        self.response_time = response_time

class Event:
    def __init__(self, client_id: int, is_return: bool, value: Any, event_id: int):
        # Optional, unless you want a visualization; zero-indexed
        self.client_id = client_id
        self.is_return = is_return
        self.value = value
        self.event_id = event_id

class Model:
    def __init__(self, partition: Callable[[List[Operation]], List[List[Operation]]] = None,
                       partition_event: Callable[[List[Event]], List[List[Event]]] = None,
                       init: Callable[[], Any] = None,
                       step: Callable[[Any, Any, Any], Tuple[bool, Any]] = None,
                       equal: Callable[[Any, Any], bool] = None,
                       describe_operation: Callable[[Any, Any], str] = None,
                       describe_state: Callable[[Any], str] = None):
        # Partition functions, such that a history is linearizable if and only
        # if each partition is linearizable. If you don't want to implement
        # this, you can always use the `no_partition` functions implemented
        # below.
        self.partition = partition
        self.partition_event = partition_event
        # Initial state of the system.
        self.init = init
        # Step function for the system. Returns whether or not the system
        # could take this step with the given inputs and outputs and also
        # returns the new state. This should not mutate the existing state.
        self.step = step
        # Equality on states. If you are using a simple data type for states,
        # you can use the `shallow_equal` function implemented below.
        self.equal = equal
        # For visualization, describe an operation as a string.
        # For example, "Get('x') -> 'y'".
        self.describe_operation = describe_operation
        # For visualization purposes, describe a state as a string.
        # For example, "{'x' -> 'y', 'z' -> 'w'}"
        self.describe_state = describe_state

def no_partition(history: List[Operation]) -> List[List[Operation]]:
    return [history]

def no_partition_event(history: List[Event]) -> List[List[Event]]:
    return [history]

def shallow_equal(state1: Any, state2: Any) -> bool:
    return state1 == state2

def default_describe_operation(input: Any, output: Any) -> str:
    return f"{input} -> {output}"

def default_describe_state(state: Any) -> str:
    return str(state)

