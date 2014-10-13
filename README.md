An asynchronous task engine implemented in python.

It can run raw tasks or activity based workflows. See worker.py and TestWorkflow.py for an example of each.

Engine provides a mechanism for following:-
1. Locks for synchronization.
2. Ability to sleep for a user defined period.
3. Message based communication between workflows.
