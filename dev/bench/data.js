window.BENCHMARK_DATA = {
  "lastUpdate": 1769805211252,
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
          "id": "8a9ffc63516ebdc9f0a49fef8c5eeeac23d2c93b",
          "message": "made task trait associate an error type (#45)\n\n* made task trait associate an error type\n\n* fixed clippies?\n\n* removed associated error type\n\n* fixed fmt",
          "timestamp": "2026-01-05T19:33:37Z",
          "tree_id": "5946e072f4e1f6097cd85f93cd7f3dfd7f77fe88",
          "url": "https://github.com/tensorzero/durable/commit/8a9ffc63516ebdc9f0a49fef8c5eeeac23d2c93b"
        },
        "date": 1767643775902,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45306216,
            "range": "± 456311",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 81464484,
            "range": "± 1036061",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 136110271,
            "range": "± 765949",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12485905,
            "range": "± 656487",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15458291,
            "range": "± 2003897",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 14597628,
            "range": "± 1632373",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 30486741,
            "range": "± 1641845",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24337390,
            "range": "± 243130",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 70954048,
            "range": "± 4891036",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 650614566,
            "range": "± 11057519",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 415247186,
            "range": "± 4293234",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 375407686,
            "range": "± 6553230",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 382654795,
            "range": "± 15463372",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 154318814,
            "range": "± 5101818",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 112511741,
            "range": "± 4353406",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 862759,
            "range": "± 421813",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 578782139,
            "range": "± 7006880",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 364398041,
            "range": "± 4224253",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 252546960,
            "range": "± 3874272",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 195544813,
            "range": "± 1462414",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15762058,
            "range": "± 558887",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "8ceb3c447ea8c77b7f6faef4e717602ad149a0f4",
          "message": "Bump opentelemetry dependencies to latest versions (#47)",
          "timestamp": "2026-01-06T18:54:10Z",
          "tree_id": "8bb77228b69812197d6a0bf8e58c358ef3e1564a",
          "url": "https://github.com/tensorzero/durable/commit/8ceb3c447ea8c77b7f6faef4e717602ad149a0f4"
        },
        "date": 1767727770379,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 32356500,
            "range": "± 1402731",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 71418142,
            "range": "± 1795535",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 113362458,
            "range": "± 1408650",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12452371,
            "range": "± 694719",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 12191256,
            "range": "± 818132",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 11752214,
            "range": "± 763231",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 23285712,
            "range": "± 521031",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24188039,
            "range": "± 112672",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 57820973,
            "range": "± 5313192",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 557611661,
            "range": "± 6483462",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 337187420,
            "range": "± 5097560",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 301353396,
            "range": "± 6037792",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 308864041,
            "range": "± 8327356",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 139890513,
            "range": "± 7746086",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 87306290,
            "range": "± 5235560",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 713068,
            "range": "± 370630",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 512395810,
            "range": "± 6031858",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 310737324,
            "range": "± 3619643",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 205837753,
            "range": "± 3171519",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 157917863,
            "range": "± 3880967",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 14146727,
            "range": "± 302520",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8597fc1345d0b45426240038bc16a6dc5e1a32ba",
          "message": "Instrument durable task execution with current span (#50)\n\nThis preserves the distibuted OTEL trace context",
          "timestamp": "2026-01-07T20:00:32Z",
          "tree_id": "ac9f33572b68c58e9eb73b0ea9d07f45bbefce3c",
          "url": "https://github.com/tensorzero/durable/commit/8597fc1345d0b45426240038bc16a6dc5e1a32ba"
        },
        "date": 1767818209280,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 44227558,
            "range": "± 711290",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 81413161,
            "range": "± 907229",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 135354198,
            "range": "± 1076183",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12936864,
            "range": "± 734097",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15367271,
            "range": "± 1586414",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 13885724,
            "range": "± 1172056",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 29979486,
            "range": "± 1181277",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24318602,
            "range": "± 496126",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 71458992,
            "range": "± 3602623",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 646414615,
            "range": "± 11089400",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 411950197,
            "range": "± 2940670",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 378874976,
            "range": "± 5762247",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 383931857,
            "range": "± 5455716",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 156321928,
            "range": "± 6560938",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 113760331,
            "range": "± 5163221",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 855379,
            "range": "± 433820",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 570537372,
            "range": "± 8042171",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 363478855,
            "range": "± 3613066",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 253591116,
            "range": "± 2876930",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 200154577,
            "range": "± 3063258",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15844783,
            "range": "± 573369",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "12f5773d957db49902ca6fdbe7b0a4688a0d9a5d",
          "message": "Create an OpenTelemetry link instead of setting parent (#53)\n\nThe opentelemetry docs specifically call out our use-case\n(queueing an asynchronous task) as a use case for links:\nhttps://opentelemetry.io/docs/concepts/signals/traces/#span-links",
          "timestamp": "2026-01-08T14:59:43Z",
          "tree_id": "4bd17742ed51828f683b398a48ae8cd478b7cb55",
          "url": "https://github.com/tensorzero/durable/commit/12f5773d957db49902ca6fdbe7b0a4688a0d9a5d"
        },
        "date": 1767886508932,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45960790,
            "range": "± 696795",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 82649225,
            "range": "± 1684963",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 136398320,
            "range": "± 532677",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12780657,
            "range": "± 703475",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15564643,
            "range": "± 1074920",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 13178738,
            "range": "± 1166925",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 31220427,
            "range": "± 650036",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24788695,
            "range": "± 451552",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 72598398,
            "range": "± 5363214",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 649379687,
            "range": "± 13430107",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 423331420,
            "range": "± 9777842",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 375065727,
            "range": "± 4796089",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 386216482,
            "range": "± 6850939",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 160229960,
            "range": "± 5095035",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 116097732,
            "range": "± 4912235",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 929985,
            "range": "± 676929",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 598293819,
            "range": "± 9947296",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 371628364,
            "range": "± 3872031",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 255871127,
            "range": "± 2495240",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 200056044,
            "range": "± 2904504",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15837995,
            "range": "± 634601",
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
          "id": "9e415cd3610c496cb0b43bd844821b6559a6ac81",
          "message": "bumped wait for task to start (#54)",
          "timestamp": "2026-01-08T15:10:45Z",
          "tree_id": "d16718cd56a5e730782fdc1842b4303e8b144216",
          "url": "https://github.com/tensorzero/durable/commit/9e415cd3610c496cb0b43bd844821b6559a6ac81"
        },
        "date": 1767887232494,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45317830,
            "range": "± 1404064",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 81979229,
            "range": "± 1067806",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 136650512,
            "range": "± 1895172",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 13476136,
            "range": "± 984938",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15749349,
            "range": "± 1159058",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 13355735,
            "range": "± 1105556",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 30438187,
            "range": "± 1009622",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24232402,
            "range": "± 346035",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 70268817,
            "range": "± 3565632",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 655354041,
            "range": "± 9978333",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 419271965,
            "range": "± 7247218",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 379095379,
            "range": "± 13814459",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 388427274,
            "range": "± 7236783",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 161634325,
            "range": "± 7657393",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 113230938,
            "range": "± 3826988",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 872135,
            "range": "± 427822",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 581970195,
            "range": "± 8767382",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 364510369,
            "range": "± 5405021",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 253364880,
            "range": "± 2982898",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 198372597,
            "range": "± 3480401",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15861269,
            "range": "± 575274",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a198f9b013d6331023584283bcba83f5c2761f83",
          "message": "Revert \"Create an OpenTelemetry link instead of setting parent (#53)\" (#55)\n\nSentry and AWS X-ray don't handle links properly, so let's switch\nback to setting the parent for now",
          "timestamp": "2026-01-09T18:24:56Z",
          "tree_id": "1c498874c59efb43f15947ba7fffbfc46378570e",
          "url": "https://github.com/tensorzero/durable/commit/a198f9b013d6331023584283bcba83f5c2761f83"
        },
        "date": 1767985251697,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45167164,
            "range": "± 2455660",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 84878140,
            "range": "± 1675925",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 138782552,
            "range": "± 1857200",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 13140642,
            "range": "± 876855",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 14680309,
            "range": "± 1278509",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 13784303,
            "range": "± 2148464",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 30075252,
            "range": "± 1790429",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24247638,
            "range": "± 162407",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 67029681,
            "range": "± 6746060",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 659491318,
            "range": "± 10533358",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 414477093,
            "range": "± 4198350",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 371355288,
            "range": "± 9149872",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 381445440,
            "range": "± 9685515",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 162852028,
            "range": "± 9180431",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 115308840,
            "range": "± 4501009",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 947163,
            "range": "± 639402",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 584322282,
            "range": "± 5679027",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 365028955,
            "range": "± 3057923",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 252829449,
            "range": "± 1496230",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 197360017,
            "range": "± 1858746",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15830901,
            "range": "± 585601",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "75bde52f2a40d1fabd72443ddcf26e1342e28b56",
          "message": "Lower 'durable.worker.claim_tasks' span to 'debug' level (#56)\n\nWe already have the metric, so let's avoid reporting lots of spans\nto OpenTelemetry",
          "timestamp": "2026-01-09T18:32:17Z",
          "tree_id": "7456889ed68495c89e33125134ef007090ef4818",
          "url": "https://github.com/tensorzero/durable/commit/75bde52f2a40d1fabd72443ddcf26e1342e28b56"
        },
        "date": 1767985680992,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45099207,
            "range": "± 2367929",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 86607211,
            "range": "± 4406823",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 140495839,
            "range": "± 3942045",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12868435,
            "range": "± 1120462",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 13907909,
            "range": "± 1343443",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 14711286,
            "range": "± 1729031",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 30243262,
            "range": "± 1586031",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24093928,
            "range": "± 112362",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 70796459,
            "range": "± 4902681",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 659303178,
            "range": "± 15869336",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 416983988,
            "range": "± 18352696",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 371574937,
            "range": "± 6275993",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 378298671,
            "range": "± 8242106",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 156377995,
            "range": "± 6740937",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 107818055,
            "range": "± 5520171",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 911055,
            "range": "± 632086",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 586218953,
            "range": "± 7770286",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 362848467,
            "range": "± 4045687",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 251393858,
            "range": "± 4183311",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 196626967,
            "range": "± 1971175",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15714231,
            "range": "± 486170",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "45734a1c5e62deea63524c44989e46a1b29f797f",
          "message": "Validate task parameters in 'spawn_by_name' (#57)\n\n* Validate task parameters in 'spawn_by_name'\n\nThe 'spawn_by_name' method requires the task to exist\nin the registry, so we can validate the parameters by attempting\nto deserialize into the parameter type.\n\nThis lets us catch some errors before we try to insert the task\ninto the database\n\n* Fix test\n\n* Fix test_spawn_with_empty_params",
          "timestamp": "2026-01-18T16:31:08Z",
          "tree_id": "37a72a31761bfb611d3206e45842cd58b742d832",
          "url": "https://github.com/tensorzero/durable/commit/45734a1c5e62deea63524c44989e46a1b29f797f"
        },
        "date": 1768756015185,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 43229315,
            "range": "± 1600095",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 81308389,
            "range": "± 2231243",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 135929500,
            "range": "± 1286764",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12624770,
            "range": "± 1114632",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 14531638,
            "range": "± 1643939",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 14135922,
            "range": "± 1787643",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 30789404,
            "range": "± 837240",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24174578,
            "range": "± 173032",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 71825496,
            "range": "± 8584504",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 637487013,
            "range": "± 7221567",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 409505779,
            "range": "± 9354941",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 372262529,
            "range": "± 7802926",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 382368200,
            "range": "± 9727324",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 158970111,
            "range": "± 6304234",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 115744666,
            "range": "± 7254654",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 849566,
            "range": "± 417045",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 566356385,
            "range": "± 5517778",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 358374723,
            "range": "± 5562832",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 251266696,
            "range": "± 4166667",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 195217569,
            "range": "± 3828072",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15694497,
            "range": "± 389890",
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
          "id": "851d60d5d61d749319d2c467abc012805daf76d6",
          "message": "changed interface to use instance methods rather than static ones (#58)\n\n* changed interface to use instance mthods rather than static ones\n\n* changed interface to use instance mthods rather than static ones",
          "timestamp": "2026-01-20T16:12:21Z",
          "tree_id": "bc05ffacc4b0dd39c99315f61e1421f999cb2d20",
          "url": "https://github.com/tensorzero/durable/commit/851d60d5d61d749319d2c467abc012805daf76d6"
        },
        "date": 1768927797033,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 46274775,
            "range": "± 2399164",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 85316043,
            "range": "± 2475263",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 137823807,
            "range": "± 1934237",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 13363002,
            "range": "± 932936",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15113686,
            "range": "± 2167098",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 14867715,
            "range": "± 1901296",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 31622090,
            "range": "± 774602",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24399953,
            "range": "± 247135",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 74405335,
            "range": "± 4244097",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 670684087,
            "range": "± 8027042",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 423252391,
            "range": "± 6432182",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 389968957,
            "range": "± 8324732",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 393811986,
            "range": "± 6780516",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 163171644,
            "range": "± 6504228",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 115034241,
            "range": "± 4556375",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 924056,
            "range": "± 691097",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 629575698,
            "range": "± 11372140",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 379119854,
            "range": "± 4385947",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 260180401,
            "range": "± 3565603",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 211188230,
            "range": "± 3849201",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 16094136,
            "range": "± 590423",
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
          "id": "50d6324876cf20d01c55d800ddbfe524ec1899fc",
          "message": "add infrastructure for migrations (#60)\n\n* add infrastructure for migrations\n\n* changed pg validation version to 14",
          "timestamp": "2026-01-22T15:27:39Z",
          "tree_id": "b237ae19d53c88fd7ef01071fcba965fe2577b61",
          "url": "https://github.com/tensorzero/durable/commit/50d6324876cf20d01c55d800ddbfe524ec1899fc"
        },
        "date": 1769097832740,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 46208281,
            "range": "± 1959458",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 81877044,
            "range": "± 1000476",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 136406523,
            "range": "± 1303355",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 12124293,
            "range": "± 987567",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15753397,
            "range": "± 1881875",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 14896339,
            "range": "± 1711589",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 30843484,
            "range": "± 1381885",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24431626,
            "range": "± 251169",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 70945212,
            "range": "± 3993906",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 660087965,
            "range": "± 10014033",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 412872912,
            "range": "± 6239379",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 382166727,
            "range": "± 12121545",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 396893499,
            "range": "± 15247291",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 167256889,
            "range": "± 9949295",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 111697847,
            "range": "± 4988057",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 863442,
            "range": "± 448039",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 603436073,
            "range": "± 10732904",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 370677446,
            "range": "± 5437277",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 257447503,
            "range": "± 4492602",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 200325113,
            "range": "± 1934865",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15919303,
            "range": "± 604010",
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
          "id": "3a06598377de257ba27dc1e5be2aa44f92b1dd49",
          "message": "Improve indices for common queries (#61)\n\n* add a migration adding an index for claim task + infra for doing migrations nicely\n\n* attempt at fixing migration\n\n* make them match\n\n* added all indices required for durable queries\n\n* added an index for sleeping\n\n* add infrastructure for migrations\n\n* added index for wait registrations on task_id\n\n* consolidated partial indices",
          "timestamp": "2026-01-22T17:15:57Z",
          "tree_id": "0dcc349e197551b7d96f9bd9d70154290335d2f6",
          "url": "https://github.com/tensorzero/durable/commit/3a06598377de257ba27dc1e5be2aa44f92b1dd49"
        },
        "date": 1769104335723,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 43365672,
            "range": "± 2220700",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 87376159,
            "range": "± 2230491",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 140638887,
            "range": "± 1537665",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 13673352,
            "range": "± 1162841",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 13343637,
            "range": "± 1075671",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 14527729,
            "range": "± 1512801",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 26901929,
            "range": "± 484581",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24691549,
            "range": "± 574628",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 72159425,
            "range": "± 5134215",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 657555123,
            "range": "± 10333574",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 420321009,
            "range": "± 8288872",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 383839629,
            "range": "± 23648868",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 391011031,
            "range": "± 17624089",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 163689628,
            "range": "± 6753675",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 119305379,
            "range": "± 6461555",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 858330,
            "range": "± 455836",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 626163324,
            "range": "± 6531432",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 371482620,
            "range": "± 6630348",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 257121810,
            "range": "± 2889100",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 201416944,
            "range": "± 1871613",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15839844,
            "range": "± 440808",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "b779f5a3f7e34f4f8a758ec4627d56538945cfeb",
          "message": "Refactor TaskContext to hold and use a Durable client (#62)\n\n* Refactor TaskContext to hold and use a Durable client\n\nWe now delegate to the existing 'spawn_by_name' (wrapping it\nin a checkpoint in TaskContext). This lets us re-use all of\nthe existing logic, including the OpenTelemetry context propagation\nlogic. This will give us a tree structure in OpenTelemetry - subtasks\nwill use their parent task as the parent trace\n\n* Run clippy\n\n* Fix telemetry\n\n* Set parent_task_id\n\n* Fix review comments\n\n* Mark owns_pool as false when client is cloesd",
          "timestamp": "2026-01-23T15:36:43Z",
          "tree_id": "c47cfaaad8cdaa857e42a682ff46d1dc62a2340e",
          "url": "https://github.com/tensorzero/durable/commit/b779f5a3f7e34f4f8a758ec4627d56538945cfeb"
        },
        "date": 1769184742452,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 43493151,
            "range": "± 2494221",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 90412794,
            "range": "± 1627186",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 144621866,
            "range": "± 3425211",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 15415207,
            "range": "± 2073279",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 14483193,
            "range": "± 1319306",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 13929521,
            "range": "± 2173711",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 27318257,
            "range": "± 577044",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24922949,
            "range": "± 481456",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 73352747,
            "range": "± 5067647",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 661835431,
            "range": "± 11878759",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 432662883,
            "range": "± 7797009",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 387289819,
            "range": "± 5484993",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 397702524,
            "range": "± 8383644",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 165975610,
            "range": "± 6489987",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 119439015,
            "range": "± 6662465",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 931060,
            "range": "± 687795",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 624514176,
            "range": "± 6418717",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 383914777,
            "range": "± 6655902",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 267449648,
            "range": "± 4959880",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 206539122,
            "range": "± 4026186",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15917519,
            "range": "± 594979",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3eb32595a92dce26fad069e65d33b60f9f197363",
          "message": "Add wrapper struct to durable events and store trace context (#65)\n\n* Add wrapper struct to durable events and store trace context\n\nThis should result in Sentry displaying a link from the task\nexecution trace (where 'await_event' is called) back to the trace\nthat performed the 'emit_event' call\n\nNote that this is a *breaking change*, as we now wrap the user's payload\nin a struct when reading/writing to the database. Going forward, we'll\nbe able to add new (optional) fields to this wrapper struct without\nbreaking existing durable deployments\n\n* Run fmt\n\n* Fix metadata\n\n* Fix optional\n\n* Run fmt\n\n* Enforce that 'inner' and 'metadata' exist\n\n* Call process_event_payload_wrapper",
          "timestamp": "2026-01-29T17:34:11Z",
          "tree_id": "74725882b58e8cfa4b29fe048707d8f6a8f154ae",
          "url": "https://github.com/tensorzero/durable/commit/3eb32595a92dce26fad069e65d33b60f9f197363"
        },
        "date": 1769710230616,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 43841161,
            "range": "± 2236134",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 86060190,
            "range": "± 2694218",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 140662007,
            "range": "± 1529989",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 14099556,
            "range": "± 1325341",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 13988951,
            "range": "± 1403900",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 16970356,
            "range": "± 1907030",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 26439380,
            "range": "± 850574",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 24727072,
            "range": "± 247376",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 73196129,
            "range": "± 5863036",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 656767604,
            "range": "± 13127611",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 432637526,
            "range": "± 7519018",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 394947781,
            "range": "± 8480553",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 392344084,
            "range": "± 5719528",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 162069574,
            "range": "± 9801371",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 119077160,
            "range": "± 4992327",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 912824,
            "range": "± 664635",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 617256629,
            "range": "± 5423279",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 380023239,
            "range": "± 2567524",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 262143175,
            "range": "± 4071822",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 205763960,
            "range": "± 1671450",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 15936612,
            "range": "± 801195",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8248424b4dc89c68695e24f4e923282a70849ea3",
          "message": "Fail durable tasks immediately for non-retryable errors (#66)\n\n* Fail durable tasks immediately for non-retryable errors\n\nCurrently, we classify only a few error types (including\nerrors from user steps) as retryable. Everything else is non-retryable,\nand causes the task to fail immediately, without any retries\n\n* Run fmt",
          "timestamp": "2026-01-29T22:50:36Z",
          "tree_id": "dbbb3e1503363497432f94a38898345def15e8ef",
          "url": "https://github.com/tensorzero/durable/commit/8248424b4dc89c68695e24f4e923282a70849ea3"
        },
        "date": 1769729190270,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 45960000,
            "range": "± 2092572",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 92706875,
            "range": "± 967579",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 148406756,
            "range": "± 844368",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 14304981,
            "range": "± 1017722",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15152730,
            "range": "± 1409133",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 15607276,
            "range": "± 1708670",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 27342521,
            "range": "± 507692",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 25178975,
            "range": "± 359663",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 62795429,
            "range": "± 6302374",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 677876119,
            "range": "± 11312805",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 430926696,
            "range": "± 5863647",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 388699310,
            "range": "± 4944456",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 393755160,
            "range": "± 7705237",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 171823399,
            "range": "± 11188372",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 120320658,
            "range": "± 3599843",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 970919,
            "range": "± 693577",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 641517844,
            "range": "± 4320072",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 391146170,
            "range": "± 5286611",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 267580167,
            "range": "± 3390721",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 209529407,
            "range": "± 3510385",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 16016960,
            "range": "± 509479",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "aaron@tensorzero.com",
            "name": "Aaron Hill",
            "username": "Aaron1011"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "690b50734a4b14ddc63d1de60c91f9bf8ae6942f",
          "message": "Add 'cached' field to durable.task.step span (#69)\n\n* Add 'cached' field to durable.task.step span\n\n* Add feature gating",
          "timestamp": "2026-01-30T19:57:10Z",
          "tree_id": "6f0778d6cc4a50e4e56a24d513dc4572d478422f",
          "url": "https://github.com/tensorzero/durable/commit/690b50734a4b14ddc63d1de60c91f9bf8ae6942f"
        },
        "date": 1769805210990,
        "tool": "cargo",
        "benches": [
          {
            "name": "step_cache_miss/steps/10",
            "value": 47568629,
            "range": "± 2865333",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/50",
            "value": 92644224,
            "range": "± 965309",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_miss/steps/100",
            "value": 142889273,
            "range": "± 2739441",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/10",
            "value": 15566912,
            "range": "± 1187283",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/50",
            "value": 15179517,
            "range": "± 1032607",
            "unit": "ns/iter"
          },
          {
            "name": "step_cache_hit/steps/100",
            "value": 15672296,
            "range": "± 1361642",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1",
            "value": 34486059,
            "range": "± 1201800",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/100",
            "value": 26705775,
            "range": "± 715543",
            "unit": "ns/iter"
          },
          {
            "name": "large_payload_checkpoint/size_kb/1000",
            "value": 78005422,
            "range": "± 3154392",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/2",
            "value": 663374480,
            "range": "± 8324417",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/4",
            "value": 419416840,
            "range": "± 8377066",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/8",
            "value": 377114875,
            "range": "± 6166697",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_claims/workers/16",
            "value": 383250827,
            "range": "± 7721690",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/baseline",
            "value": 164087549,
            "range": "± 8220295",
            "unit": "ns/iter"
          },
          {
            "name": "claim_latency/scenario/contention",
            "value": 114431401,
            "range": "± 5129314",
            "unit": "ns/iter"
          },
          {
            "name": "spawn_latency/single_spawn",
            "value": 829871,
            "range": "± 485437",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/1",
            "value": 619643373,
            "range": "± 4779109",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/2",
            "value": 384216129,
            "range": "± 1737421",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/4",
            "value": 264459270,
            "range": "± 3279270",
            "unit": "ns/iter"
          },
          {
            "name": "task_throughput/workers/8",
            "value": 209554219,
            "range": "± 2470966",
            "unit": "ns/iter"
          },
          {
            "name": "e2e_completion/single_task_roundtrip",
            "value": 16150125,
            "range": "± 525516",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}