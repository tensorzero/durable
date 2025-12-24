window.BENCHMARK_DATA = {
  "lastUpdate": 1766604101466,
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
      }
    ]
  }
}