window.BENCHMARK_DATA = {
  "lastUpdate": 1767323644796,
  "repoUrl": "https://github.com/tensorzero/durable",
  "entries": {
    "Criterion Benchmark": [
      {
        "commit": {
          "author": {
            "email": "virajmehta@users.noreply.github.com",
            "name": "Viraj Mehta",
            "username": "virajmehta"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "6984ee936b8315ead4199cc66646bfaf0c76e134",
          "message": "remove lib from bench (#33)",
          "timestamp": "2025-12-20T21:54:12Z",
          "tree_id": "38dc52b048f97fbc3f20ed1ad982cda0fb93973e",
          "url": "https://github.com/tensorzero/durable/commit/6984ee936b8315ead4199cc66646bfaf0c76e134"
        },
        "date": 1766269792917,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45777293,
            "range": "± 1134698",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 88339670,
            "range": "± 1251081",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 141958415,
            "range": "± 2081004",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 11903802,
            "range": "± 325622",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 11900051,
            "range": "± 315927",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 12463581,
            "range": "± 874761",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 27151450,
            "range": "± 1060688",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24050055,
            "range": "± 147277",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 69605252,
            "range": "± 5080768",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 607433353,
            "range": "± 12966933",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 391994472,
            "range": "± 7841746",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 352187549,
            "range": "± 8354309",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 352974746,
            "range": "± 6745117",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 155295765,
            "range": "± 44683730",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 108042169,
            "range": "± 8025244",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 845348,
            "range": "± 424105",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 543567814,
            "range": "± 2375919",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 326452095,
            "range": "± 2615723",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 242154640,
            "range": "± 2872590",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 184134735,
            "range": "± 3014974",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 14729507,
            "range": "± 394287",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "virajmehta@users.noreply.github.com",
            "name": "Viraj Mehta",
            "username": "virajmehta"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "551f3cb4e672ca854cad1b35d912d08e7bf55607",
          "message": "fix: prevent lost wakeups in event await/emit race condition (#37)\n\nAdd advisory locks to serialize concurrent await_event and emit_event\n   operations on the same event. This prevents a race condition where:\n\n   1. Task A checks if event exists (not yet)\n   2. Task B emits the event and wakes waiters (none yet)\n   3. Task A registers as a waiter (missed the wake)\n\n   The lock_event() function uses pg_advisory_xact_lock with hashed\n   queue_name and event_name to ensure atomicity.\n\n   Also changes emit_event to first-writer-wins semantics (ON CONFLICT\n   DO NOTHING) to maintain consistency - subsequent emits for the same\n   event are no-ops.\n\n   Tests:\n   - test_event_functions_use_advisory_locks: Verifies both functions call lock_event\n   - test_event_race_stress: Stress test with 128 concurrent tasks x 4 rounds\n   - test_event_first_writer_wins: Renamed from test_event_last_write_wins",
          "timestamp": "2025-12-21T17:20:48Z",
          "tree_id": "cfe32e48838e112288e590520821d8a5ecf71de5",
          "url": "https://github.com/tensorzero/durable/commit/551f3cb4e672ca854cad1b35d912d08e7bf55607"
        },
        "date": 1766342951349,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45839945,
            "range": "± 1322907",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 90054953,
            "range": "± 1479827",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 143503209,
            "range": "± 2079548",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12008902,
            "range": "± 282984",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 11750280,
            "range": "± 419605",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 11742166,
            "range": "± 394732",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 28931091,
            "range": "± 1860200",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24247906,
            "range": "± 188847",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 73119604,
            "range": "± 4387663",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 618542688,
            "range": "± 10484028",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 401977378,
            "range": "± 8610861",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 354156492,
            "range": "± 6320041",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 357871998,
            "range": "± 6839477",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 157521455,
            "range": "± 10369272",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 103957700,
            "range": "± 5263760",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 858154,
            "range": "± 442857",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 556289706,
            "range": "± 11219761",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 326909829,
            "range": "± 5966224",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 244252825,
            "range": "± 3639529",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 188313896,
            "range": "± 3668630",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 14843282,
            "range": "± 437462",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "virajmehta@users.noreply.github.com",
            "name": "Viraj Mehta",
            "username": "virajmehta"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "82e330b40c534a842c674a2163c94d1272c5e9d8",
          "message": "changed durable's API to use rust durations (#41)\n\n* changed durables API to use rust durations\n\n* prevent durations that are subsecond for claim timeout\n\n* formatted\n\n* fixed semantic conflicts",
          "timestamp": "2025-12-24T18:46:12Z",
          "tree_id": "6d917ddf67a976219d237da916dc85f68e424518",
          "url": "https://github.com/tensorzero/durable/commit/82e330b40c534a842c674a2163c94d1272c5e9d8"
        },
        "date": 1766604101105,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 44311499,
            "range": "± 3301643",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 84030267,
            "range": "± 1887017",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 136212753,
            "range": "± 573697",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 11901164,
            "range": "± 517052",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 11808811,
            "range": "± 287729",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 11755690,
            "range": "± 644680",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 26131315,
            "range": "± 685131",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24092525,
            "range": "± 267449",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 57924643,
            "range": "± 8029373",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 595621752,
            "range": "± 7797122",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 383726625,
            "range": "± 5169603",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 344534132,
            "range": "± 5663005",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 351559920,
            "range": "± 7364064",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 150885087,
            "range": "± 6215670",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 104957641,
            "range": "± 4239259",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 830463,
            "range": "± 434268",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 539264871,
            "range": "± 3489150",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 308356017,
            "range": "± 2740453",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 236229706,
            "range": "± 2503563",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 181660217,
            "range": "± 1904617",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 14744272,
            "range": "± 385795",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "virajmehta@users.noreply.github.com",
            "name": "Viraj Mehta",
            "username": "virajmehta"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e378ba1ee9b9c6cc2a020aa21b75f0c35b529bc8",
          "message": "improved fix for event race and added a better test (#42)\n\n* improved fix for event race and added a better test based on https://github.com/earendil-works/absurd/pull/61\n\n* removed timestamp set\n\n* removed unnecessary test\n\n* fixed issue with overwriting event\n\n* fixed issue with overwriting event\n\n* merged",
          "timestamp": "2025-12-24T19:27:02Z",
          "tree_id": "4e2da30940cc819ed2fe0745581ea2536428fc99",
          "url": "https://github.com/tensorzero/durable/commit/e378ba1ee9b9c6cc2a020aa21b75f0c35b529bc8"
        },
        "date": 1766606580268,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 46385721,
            "range": "± 1097340",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 91767920,
            "range": "± 1029354",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 149403919,
            "range": "± 1192100",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12047569,
            "range": "± 269255",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 12134492,
            "range": "± 422911",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 12038790,
            "range": "± 534753",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 29273543,
            "range": "± 1415905",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24180834,
            "range": "± 247064",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 72122109,
            "range": "± 5819430",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 625173997,
            "range": "± 10793584",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 397453693,
            "range": "± 7927981",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 360067808,
            "range": "± 4367351",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 363334873,
            "range": "± 6280254",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 156870056,
            "range": "± 6855248",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 108749311,
            "range": "± 10436280",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 924002,
            "range": "± 685623",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 562913667,
            "range": "± 5749060",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 350284224,
            "range": "± 4524069",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 252613356,
            "range": "± 3167149",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 194481172,
            "range": "± 2664911",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 14780129,
            "range": "± 537872",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "virajmehta@users.noreply.github.com",
            "name": "Viraj Mehta",
            "username": "virajmehta"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "d564532eb8e46d74db028013e8d3e4dcb0d41d6d",
          "message": "fix: lock ordering & cleanup (also contains atomic checkpoints) (#38)\n\n* fix: prevent lost wakeups in event await/emit race condition\n\n   Add advisory locks to serialize concurrent await_event and emit_event\n   operations on the same event. This prevents a race condition where:\n\n   1. Task A checks if event exists (not yet)\n   2. Task B emits the event and wakes waiters (none yet)\n   3. Task A registers as a waiter (missed the wake)\n\n   The lock_event() function uses pg_advisory_xact_lock with hashed\n   queue_name and event_name to ensure atomicity.\n\n   Also changes emit_event to first-writer-wins semantics (ON CONFLICT\n   DO NOTHING) to maintain consistency - subsequent emits for the same\n   event are no-ops.\n\n   Tests:\n   - test_event_functions_use_advisory_locks: Verifies both functions call lock_event\n   - test_event_race_stress: Stress test with 128 concurrent tasks x 4 rounds\n   - test_event_first_writer_wins: Renamed from test_event_last_write_wins\n\n* Lock Ordering + Cleanup Consolidation\n\n  Problem\n\n  1. Deadlock risk: Functions that touch both tasks and runs could deadlock if they acquired locks in inconsistent order\n  2. Scattered cleanup logic: Terminal task cleanup (deleting waiters, emitting parent events, cascading cancellation) was duplicated across multiple functions\n  3. Incomplete cascade cancellation: Auto-cancelled tasks (via max_duration) didn't cascade cancel their children\n\n  Solution\n\n  Lock Ordering: All functions now acquire locks in consistent order (task first, then run):\n  - complete_run, fail_run, sleep_for, await_event: lock task FOR UPDATE before locking run\n  - claim_task: lock task before run when handling expired claims\n  - emit_event: lock sleeping tasks before waking runs (via locked_tasks CTE)\n\n  Cleanup Consolidation: New cleanup_task_terminal() function handles:\n  - Deleting wait registrations for the task\n  - Emitting completion event for parent ($child:<task_id>)\n  - Optionally cascading cancellation to children\n\n  Used by: complete_run, fail_run, cancel_task, cascade_cancel_children, claim_task\n\n  Other improvements:\n  - emit_event early return when event already exists (optimization)\n  - sleep_for now takes task_id parameter for proper lock ordering\n\n  Tests\n\n  - New lock_order_test.rs with 6 tests verifying lock ordering works correctly\n  - New test_cascade_cancel_when_parent_auto_cancelled_by_max_duration in fanout_test.rs\n  - New test helpers: single_conn_pool(), RunInfo, get_runs_for_task()\n\n* pass task ID to more endpoints so they can lock in order efficiently\n\n* make checkpoint writing atomically check the version\n\n* added test that covers lock order case for events / claim task\n\n* fmtted\n\n* fixed fmt\n\n---------\n\nCo-authored-by: Gabriel Bianconi <1275491+GabrielBianconi@users.noreply.github.com>",
          "timestamp": "2025-12-24T19:32:59Z",
          "tree_id": "8654241134d91191976b7b11a1fb7304304af5ce",
          "url": "https://github.com/tensorzero/durable/commit/d564532eb8e46d74db028013e8d3e4dcb0d41d6d"
        },
        "date": 1766606937339,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 43619651,
            "range": "± 5305992",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 80189248,
            "range": "± 479528",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 135048259,
            "range": "± 3013668",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 11995063,
            "range": "± 448811",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 14270909,
            "range": "± 1762481",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 13080880,
            "range": "± 976816",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 29834613,
            "range": "± 864038",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24148052,
            "range": "± 107861",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 71193119,
            "range": "± 5417028",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 642205183,
            "range": "± 11346326",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 408487139,
            "range": "± 7623305",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 368573483,
            "range": "± 4525308",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 384008193,
            "range": "± 9361740",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 156979789,
            "range": "± 6985531",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 115561127,
            "range": "± 7034385",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 850049,
            "range": "± 421082",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 561347159,
            "range": "± 8155801",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 350899357,
            "range": "± 2779874",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 249181617,
            "range": "± 2137879",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 192743770,
            "range": "± 1761295",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15671491,
            "range": "± 325488",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "virajmehta@users.noreply.github.com",
            "name": "Viraj Mehta",
            "username": "virajmehta"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "161e9e5479c1ba87d6310fcd8b276f4b9f7444d6",
          "message": "added a public API for spawning unchecked tasks (#43)",
          "timestamp": "2025-12-27T19:49:33Z",
          "tree_id": "8a12883701f7d0c44b9f18262120646827a77993",
          "url": "https://github.com/tensorzero/durable/commit/161e9e5479c1ba87d6310fcd8b276f4b9f7444d6"
        },
        "date": 1766867127327,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 44095771,
            "range": "± 857324",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 84616788,
            "range": "± 1710738",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 137232012,
            "range": "± 2599763",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 13435026,
            "range": "± 946603",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 14966289,
            "range": "± 906384",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 15625306,
            "range": "± 1661738",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 31794886,
            "range": "± 907598",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24333967,
            "range": "± 259184",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 71897088,
            "range": "± 4582901",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 649995179,
            "range": "± 10631903",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 416349419,
            "range": "± 8740576",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 377686819,
            "range": "± 6290569",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 383824514,
            "range": "± 13335197",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 158956643,
            "range": "± 9836296",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 115129673,
            "range": "± 5218638",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 932500,
            "range": "± 627658",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 588718762,
            "range": "± 4514013",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 365178460,
            "range": "± 3898862",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 252860784,
            "range": "± 3101752",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 196913356,
            "range": "± 1691278",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15780131,
            "range": "± 570446",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "virajmehta@users.noreply.github.com",
            "name": "Viraj Mehta",
            "username": "virajmehta"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ca986b6042829d924d413fc0f42d3bbd87b2df70",
          "message": "Allow fully configurable defaults for tool spawn settings (#44)\n\n* propagate default num attempts to spawned tasks\n\n* add full defaults for task spawning setting\n\n* added a SpawnDefaults struct\n\n* deflake",
          "timestamp": "2026-01-02T02:38:03Z",
          "tree_id": "abc8461822b32f10fbd871378a260962509504d5",
          "url": "https://github.com/tensorzero/durable/commit/ca986b6042829d924d413fc0f42d3bbd87b2df70"
        },
        "date": 1767323643941,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45704454,
            "range": "± 2245721",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 91140120,
            "range": "± 2162769",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 147259349,
            "range": "± 2497716",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 14505224,
            "range": "± 1602846",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 14716579,
            "range": "± 2044193",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 16783505,
            "range": "± 1036779",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 30139615,
            "range": "± 1055044",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 25427507,
            "range": "± 403581",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 71192245,
            "range": "± 4307824",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 692338513,
            "range": "± 18270475",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 434902823,
            "range": "± 9856424",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 389626727,
            "range": "± 7418417",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 395894989,
            "range": "± 25255203",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 169369529,
            "range": "± 7817615",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 119382223,
            "range": "± 4826447",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 1057856,
            "range": "± 675347",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 636930439,
            "range": "± 7523082",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 397703090,
            "range": "± 4532205",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 273092362,
            "range": "± 2267123",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 208457419,
            "range": "± 2631406",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 16142948,
            "range": "± 628676",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}