
## 2026-11-20 FlowScope: memory-per-flow experiment, grid and detail popup

- **Per-thread allocation cannot be read from FlowScope's hooks.** Measured on Mendix 10.24 / JDK 21: the
  MicroflowEngine log subscriber runs on `Thread-0` and the IProfiler database hook on `ProfileUtil-N`; the
  request itself runs on `JettyServer-N`. `ThreadMXBean.getThreadAllocatedBytes` is per thread, so neither hook
  can read it for the flow. `IProfiler.enterRuntime` was never called, and a `UserActionListener` registered
  through `Core.getListenersRegistry()` never fired. Experiment code was removed again.
- Local run without PostgreSQL: `./mxcli run --local --db-type hsqldb --detach`. Security is effectively off
  locally (only the Anonymous user exists), the demo-user login fails with "unknown user" and is not needed.
- Executions overview is now a Data grid 2 (sort + per-column filters) with a detail popup
  (`FlowScope.Execution_Detail`); `IsExpanded` and `NAN_ToggleExpanded` dropped.
- Regression caught by testing: `RunWriter` still set `IsExpanded` after the attribute was dropped, which made
  every save fail silently (nothing captured). Fixed.
- Verified in the running app: detail popup scrolls, sorting asc/desc on Duration, text filter on Microflow,
  Playground scenarios captured (fast one correctly not). Number and date filters not clicked through.
- Heap start/end/delta removed (grid column, detail fact, attributes, capture code): the value was the whole JVM's
  heap and misled. Capture re-verified afterwards (PLAY_Slow captured). Overview, detail popup and Playground
  restyled in `themesource/flowscope/web/main.scss` only; no widget or feature removed.

## 2026-11-20 MemoryWatch: heap-pressure notes saved for later reading

- New module `MemoryWatch` (Java in `javasource/memorywatch/implementation`, MDL in `mdlsource/20-24`, tests in
  `tests/memorywatch.test.mdl`). When the old generation is still above a threshold after a GC (JVM collection
  usage threshold on the old pool), it saves one `MemoryEvent`: heap level, the microflows running, and the
  threads that allocated most in the last minute. A per-minute memory floor is saved from GC notifications.
- The running flows come from FlowScope's microflow log hook (`CaptureEngine.runningFlows()`, read by reflection,
  so FlowScope stays optional). With FlowScope capture off the event says "unknown", not "none".
- Found by testing: a heap notification arrives after the collection, so a flow that allocates and ends in
  between was missing. FlowScope now also reports flows finished in the last 3 s, marked "Finished just before".
- Found by testing: allocation per thread must include threads born since the last sample and the part since the
  last 10 s tick, or the busiest thread of a fresh spike is missed.
- The project has one after-startup slot (FlowScope's). MemoryWatch is started by a scheduled event every
  minute that re-applies the saved settings, so after a restart it is on within a minute.
- Verified in the running app (G1 Old Gen, 8 GB): single flow, two flows at once (innocent plus guilty),
  big Mendix object list, leak steps (floor), churn (busy thread, 40 GB allocated, no event), debounce, kill
  switch, FlowScope off, test event, settings applied on save. Microflow tests: 5 pass (use
  `--db-type hsqldb --app-port 9081 --admin-port 9091 --serve-port 7544`, with the dev app stopped).
- Lint score fell from 61 to 48 because the new module carries the same convention warnings as the others
  (no business keys, create/delete grants, Java calls with rollback handling in playground flows).
- Not measured: overhead under real load. The sampler reads allocation counters every 10 s without stack traces.
- Heap delta restored on request (`mdlsource/15`, `16`; Java capture code back in `CaptureEngine`/`RunState`/`RunBuilder`/
  `RunWriter`). Still the whole JVM's heap, so the detail tile is labelled "JVM heap, start to end". Adding the Data
  grid column via `ALTER PAGE` with `filter name { numberfilter }` wrapped the filter in a container and the grid
  showed "filter widget must be placed inside the column" errors at runtime (check passed): put `numberfilter` directly
  in the column instead. ALTER scripts 15/16 are not idempotent (re-exec fails on existing attributes).
  Verified in the running app: column shows 19 MB for SAMPLE_RunFlow, detail popup shows "87 to 106 MB (19)",
  ascending sort works. Number filter on the new column not clicked through.
