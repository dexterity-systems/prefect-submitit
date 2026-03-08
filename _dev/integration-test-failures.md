  ⎿  Your Prefect server is running an older version of Prefect than your client which may result in unexpected behavior. Please upgrade your Prefect server from version 3.6.13 to version 3.6.21 or higher.
     Unable to connect to 'ws://acoupa.dhcp.ipd:4296/api/events/in'. Please check your network settings to ensure websocket connections to the API are allowed. Otherwise event data (including task run data) may be lost. Reason: Unable to authenticate to the event stream. Please ensure the provided auth_to
     ken you are using is valid for this environment. . Set PREFECT_DEBUG_MODE=1 to see the full error.
     Service 'EventsWorker' failed with 4 pending items.


     ================================================================================================================================ short test summary info ================================================================================================================================
SKIPPED [1] tests/integration/test_cancel.py:171: slurm_begin not supported or job already started
XFAIL tests/integration/test_failures.py::TestSlurmFailureModes::test_unpicklable_return_raises_cleanly - submitit may serialize the result before our code sees it; behavior depends on submitit internals
XFAIL tests/integration/test_map.py::TestMapPrefectAPI::test_map_array_task_names_in_api - Task run names are set on compute nodes; may not propagate to API
XFAIL tests/integration/test_polling.py::TestPolling::test_wait_timeout_zero - Known Issue 2: timeout=0 treated as falsy, waits max_poll_time instead
FAILED tests/integration/test_submit.py::TestPrefectAPIIntegration::test_task_run_in_prefect_api - assert 0 >= 1
FAILED tests/integration/test_submit.py::TestPrefectAPIIntegration::test_task_run_name_contains_slurm_job_id - assert 0 >= 1
============================================================================================================ 2 failed, 214 passed, 1 skipped, 3 xfailed in 714.82s (0:11:54) ============================================================================================================
