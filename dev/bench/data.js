window.BENCHMARK_DATA = {
  "lastUpdate": 1766269793777,
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
      }
    ]
  }
}