
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
