// master.mjs — grid-state detector (Input AND Voltage) plus command
// dispatch to up to 3 Slaves. All settings live in code, no KVS.
// Convention: the Slave script on the target device has id=1, port 80.
//
// EVERYTHING runs off ONE repeating timer (tick). Shelly documents a hard
// limit of "no more than 5 timers used in a script"; the previous design
// asked for up to 9 (poll + config check + debounce + one ACK watchdog and
// one stagger timer per target) and silently loses whichever timer goes
// over the cap. Debounce, ACK watchdogs, the staggered turn-on queue and
// the input-config self-heal are therefore all serviced from tick().
//
// Self-heals input:0 config (type/enable/invert/factory_reset) on boot and
// every CONFIG_CHECK_SEC - see checkInputConfig().
// Out-of-band voltage (V_LOW_OFF/V_HIGH_OFF) triggers an IMMEDIATE cutoff,
// bypassing DEBOUNCE_OFF_SEC - see evaluate().

// ===== USER SETTINGS =====
// List of Slave devices (IP). Order = turn-on queue on recovery.
// Maximum 3 (Shelly allows no more than 5 concurrent RPC calls per script;
// an OFF broadcast fires one HTTP.POST per target at once).
let TARGETS = [
  "192.168.1.33"
  // "192.168.1.21",
  // "192.168.1.22"
];

// Voltage thresholds. Crossing V_LOW_OFF/V_HIGH_OFF means the incoming
// power itself is unsafe for sensitive electronics downstream - the
// master cuts the Slaves IMMEDIATELY on this, bypassing DEBOUNCE_OFF_SEC
// (see evaluate()). This is different from losing the input/contactor
// signal alone, which still waits DEBOUNCE_OFF_SEC before acting - an
// absent grid isn't actively damaging anything while we wait a couple
// seconds to confirm it, but out-of-band voltage is damaging it right now.
let V_LOW_OFF = 190;           // lower turn-off limit (instant cutoff)
let V_LOW_ON = 200;            // lower turn-on limit
let V_HIGH_OFF = 260;          // upper turn-off limit (instant cutoff)
let V_HIGH_ON = 250;           // upper turn-on limit
let V_STABLE_SEC = 60;         // stability inside the narrow band before "good"

// Timings
let POLL_MS = 1000;            // detector polling interval
// Recommended range 2-5s: below ~2s starts reacting to brief recloser
// blips that self-clear on their own (each one then costs a full 60-120s
// recovery wait below); above ~5s adds no real protection since genuine
// outages last far longer anyway - it only slows reaction to a real one.
let DEBOUNCE_OFF_SEC = 2;      // confirmation of grid loss
let DEBOUNCE_ON_SEC = 60;      // confirmation of grid return
let ACK_TIMEOUT_SEC = 5;       // HTTP request timeout to a Slave
let RETRIES = 2;               // retries when no ACK is received
let ON_GAP_SEC = 30;           // pause between sequential turn-on of Slaves
let CONFIG_CHECK_SEC = 900;    // how often input:0 config is re-verified

// Noise handling. A single contradicting reading no longer throws away a
// confirmation in progress - it must hold GLITCH_TOLERANCE_SEC to count as
// a real reversal. But glitches are counted: more than MAX_GLITCHES inside
// one confirmation means the signal itself is unstable, and an unstable
// signal is never evidence of a HEALTHY grid - only of an unhealthy one.
// So instability drops a pending turn-ON, and confirms a pending turn-OFF.
let GLITCH_TOLERANCE_SEC = 2;
let MAX_GLITCHES = 3;
// ====================================

let SCHEMA = "master-slave-v1";
let SLAVE_SCRIPT_ID = 1;
let SLAVE_PORT = 80;

let state = {
  gridPresent: null,
  pendingDecision: null,       // null | true | false - candidate being confirmed
  pendingSinceTs: 0,           // when the candidate first appeared
  glitchSinceTs: 0,            // when the current contradicting run started (0 = none)
  pendingGlitches: 0,          // tolerated interruptions so far in this candidate
  lastConfigCheckTs: 0,
  pending: [],                 // [{cmdId, tgtIdx, retries, deadlineTs, payload}]
  queue: [],                   // [{tgtIdx, sendAtTs}] - staggered turn-on
  pollTimer: null,
  voltageSource: null,
  voltageState: {
    current: null,             // null | "good" | "bad" | "recovering"
    recoveryStartTs: 0
  },
  inputWarned: false
};

// ===== UTILITIES =====

function logEvent(name, data) {
  Shelly.emitEvent(name, data);
  print(name, JSON.stringify(data));
}

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function getOwnIp() {
  let w = Shelly.getComponentStatus("wifi");
  if (w && w.sta_ip) return w.sta_ip;
  let e = Shelly.getComponentStatus("eth");
  if (e && e.ip) return e.ip;
  return "127.0.0.1";
}

function genCmdId() {
  return "c" + Math.floor(Math.random() * 0xffffffff).toString(16) +
         "-" + nowSec().toString(16);
}

function buildPayload(gridPresent) {
  return {
    cmd_id: genCmdId(),
    schema: SCHEMA,
    grid_present: gridPresent,
    ts: nowSec(),
    ack_url: "http://" + getOwnIp() + "/script/" + Script.id + "/ack"
  };
}

// ===== DETECTORS =====

function inputPresent() {
  let inp = Shelly.getComponentStatus("input", 0);
  if (inp === null) return null;
  if (inp.state === null || inp.state === undefined) {
    if (!state.inputWarned) {
      state.inputWarned = true;
      logEvent("ms_input_no_state", {
        note: "input:0.state is null/undefined - check Input.type (expected switch) or enable"
      });
    }
    return null;
  }
  state.inputWarned = false;
  return !!inp.state;
}

// Self-heal: input:0 must be type "switch", enabled, non-inverted (project
// hardware standard: contactor aux contact wired NO-only) and must not
// factory-reset the device after 5 toggles/60s (a flapping input could
// otherwise wipe the device). Called from tick() every CONFIG_CHECK_SEC.
function checkInputConfig() {
  let cfg = Shelly.getComponentConfig("input", 0);
  if (cfg === null) return;

  let diff = {};
  let changed = false;

  if (cfg.type !== "switch") { diff.type = "switch"; changed = true; }
  if (cfg.enable !== true) { diff.enable = true; changed = true; }
  if (cfg.invert !== false) { diff.invert = false; changed = true; }
  if (cfg.factory_reset !== false) { diff.factory_reset = false; changed = true; }

  if (!changed) return;

  Shelly.call("Input.SetConfig", { id: 0, config: diff },
    function (res, err, errMsg, attempted) {
      if (err !== 0) {
        logEvent("ms_input_autofix_err", { err: err, msg: errMsg, attempted: attempted });
        return;
      }
      logEvent("ms_input_autofix", { corrected: attempted });
    }, diff);
}

function detectVoltageSource() {
  let em = Shelly.getComponentStatus("em", 0);
  if (em !== null && (em.a_voltage !== undefined ||
                      em.b_voltage !== undefined ||
                      em.c_voltage !== undefined)) {
    return { type: "em" };
  }
  let em1_0 = Shelly.getComponentStatus("em1", 0);
  if (em1_0 !== null && em1_0.voltage !== undefined) {
    return { type: "em1" };
  }
  let sw = Shelly.getComponentStatus("switch", 0);
  if (sw !== null && sw.voltage !== undefined) {
    return { type: "switch" };
  }
  return null;
}

function readVoltages() {
  if (state.voltageSource === null) return null;
  let t = state.voltageSource.type;
  let v = [];
  if (t === "em") {
    let em = Shelly.getComponentStatus("em", 0);
    if (em === null) return null;
    if (em.a_voltage !== null && em.a_voltage !== undefined) v.push(em.a_voltage);
    if (em.b_voltage !== null && em.b_voltage !== undefined) v.push(em.b_voltage);
    if (em.c_voltage !== null && em.c_voltage !== undefined) v.push(em.c_voltage);
  } else if (t === "em1") {
    let s0 = Shelly.getComponentStatus("em1", 0);
    if (s0 !== null && s0.voltage !== null && s0.voltage !== undefined) v.push(s0.voltage);
    let s1 = Shelly.getComponentStatus("em1", 1);
    if (s1 !== null && s1.voltage !== null && s1.voltage !== undefined) v.push(s1.voltage);
  } else if (t === "switch") {
    let sw = Shelly.getComponentStatus("switch", 0);
    if (sw !== null && sw.voltage !== null && sw.voltage !== undefined) v.push(sw.voltage);
  }
  return v.length > 0 ? v : null;
}

function voltagePresent(nowTs) {
  let voltages = readVoltages();
  if (voltages === null) return null;

  let inWideOk = true;
  let inNarrowOk = true;
  for (let i = 0; i < voltages.length; i = i + 1) {
    let vv = voltages[i];
    if (vv < V_LOW_OFF || vv > V_HIGH_OFF) inWideOk = false;
    if (vv < V_LOW_ON || vv > V_HIGH_ON) inNarrowOk = false;
  }

  let vs = state.voltageState;

  if (vs.current === null) {
    vs.current = inWideOk ? "good" : "bad";
    logEvent("ms_voltage_init", { state: vs.current, voltages: voltages });
  }

  if (vs.current === "good") {
    if (!inWideOk) {
      vs.current = "bad";
      logEvent("ms_voltage_bad", { voltages: voltages });
    }
  } else if (vs.current === "bad") {
    if (inNarrowOk) {
      vs.current = "recovering";
      vs.recoveryStartTs = nowTs;
      logEvent("ms_voltage_recovering", { voltages: voltages });
    }
  } else if (vs.current === "recovering") {
    if (!inNarrowOk) {
      vs.current = "bad";
      logEvent("ms_voltage_bad_again", { voltages: voltages });
    } else if (nowTs - vs.recoveryStartTs >= V_STABLE_SEC) {
      vs.current = "good";
      logEvent("ms_voltage_good", { voltages: voltages });
    }
  }

  return vs.current === "good";
}

function detectGridState(nowTs) {
  let i = inputPresent();
  let v = voltagePresent(nowTs);
  if (i === null && v === null) return null;
  if (i === null) return v;
  if (v === null) return i;
  return i && v;
}

// ===== COMMAND DISPATCH =====

function findPending(cmdId) {
  for (let i = 0; i < state.pending.length; i = i + 1) {
    if (state.pending[i].cmdId === cmdId) return state.pending[i];
  }
  return null;
}

function clearPending(cmdId) {
  let out = [];
  for (let i = 0; i < state.pending.length; i = i + 1) {
    if (state.pending[i].cmdId !== cmdId) out.push(state.pending[i]);
  }
  state.pending = out;
}

function sendToTarget(tgtIdx, payload) {
  if (tgtIdx >= TARGETS.length) return;
  let url = "http://" + TARGETS[tgtIdx] + ":" + SLAVE_PORT +
            "/script/" + SLAVE_SCRIPT_ID + "/cmd";

  let p = findPending(payload.cmd_id);
  if (p === null) {
    p = {
      cmdId: payload.cmd_id, tgtIdx: tgtIdx, retries: 0,
      deadlineTs: 0, payload: payload
    };
    state.pending.push(p);
  }
  p.deadlineTs = nowSec() + ACK_TIMEOUT_SEC + 2;

  Shelly.call("HTTP.POST", {
    url: url,
    body: JSON.stringify(payload),
    timeout: ACK_TIMEOUT_SEC,
    content_type: "application/json"
  }, onCmdSent, payload.cmd_id);
}

function onCmdSent(res, err, errMsg, cmdId) {
  if (findPending(cmdId) === null) return;
  if (err !== 0) {
    logEvent("ms_send_err", { cmd_id: cmdId, err: err, msg: errMsg });
  }
}

// ACK watchdogs, serviced from tick() instead of one Timer per command.
function serviceWatchdogs(nowTs) {
  let due = [];
  for (let i = 0; i < state.pending.length; i = i + 1) {
    let p = state.pending[i];
    if (p.deadlineTs !== 0 && nowTs >= p.deadlineTs) due.push(p);
  }
  for (let j = 0; j < due.length; j = j + 1) {
    let p = due[j];
    if (p.retries < RETRIES) {
      p.retries = p.retries + 1;
      logEvent("ms_retry", { cmd_id: p.cmdId, retry: p.retries });
      sendToTarget(p.tgtIdx, p.payload);
    } else {
      logEvent("ms_failed", {
        cmd_id: p.cmdId, target: TARGETS[p.tgtIdx], payload: p.payload
      });
      clearPending(p.cmdId);
    }
  }
}

// Staggered turn-on queue, serviced from tick() instead of one Timer per
// target. The first target goes out immediately, without any timer at all.
function serviceQueue(nowTs) {
  let keep = [];
  for (let i = 0; i < state.queue.length; i = i + 1) {
    let q = state.queue[i];
    if (nowTs >= q.sendAtTs) {
      sendToTarget(q.tgtIdx, buildPayload(true));
    } else {
      keep.push(q);
    }
  }
  state.queue = keep;
}

function broadcastDecision(gridPresent) {
  logEvent("ms_decision", { grid_present: gridPresent });
  state.queue = [];
  if (gridPresent === false) {
    for (let i = 0; i < TARGETS.length; i = i + 1) {
      sendToTarget(i, buildPayload(false));
    }
    return;
  }
  let base = nowSec();
  for (let i = 0; i < TARGETS.length; i = i + 1) {
    if (i === 0) {
      sendToTarget(0, buildPayload(true));
    } else {
      state.queue.push({ tgtIdx: i, sendAtTs: base + i * ON_GAP_SEC });
    }
  }
}

// ===== DECISION =====

function clearCandidate() {
  state.pendingDecision = null;
  state.pendingSinceTs = 0;
  state.glitchSinceTs = 0;
  state.pendingGlitches = 0;
}

function commitDecision(newState) {
  clearCandidate();
  state.gridPresent = newState;
  broadcastDecision(newState);
}

function evaluate(nowTs) {
  let present = detectGridState(nowTs);

  // Out-of-band voltage is an active hazard to downstream electronics, not
  // a mere absence - skip DEBOUNCE_OFF_SEC and cut immediately.
  if (state.voltageState.current === "bad" && state.gridPresent !== false) {
    logEvent("ms_voltage_critical_cutoff", {});
    commitDecision(false);
    return;
  }

  // Signal unknown: never let a candidate ripen on data we no longer have.
  if (present === null) {
    if (state.pendingDecision !== null) {
      logEvent("ms_pending_dropped", { reason: "signal unknown" });
      clearCandidate();
    }
    return;
  }

  // Reading agrees with the committed state - it contradicts any candidate.
  if (present === state.gridPresent) {
    if (state.pendingDecision === null) return;
    if (state.glitchSinceTs === 0) {
      state.glitchSinceTs = nowTs;
      state.pendingGlitches = state.pendingGlitches + 1;
      if (state.pendingGlitches > MAX_GLITCHES) onUnstable();
      return;
    }
    if (nowTs - state.glitchSinceTs >= GLITCH_TOLERANCE_SEC) {
      clearCandidate();          // the reversal held - it is real
    }
    return;
  }

  // Reading differs from the committed state - a change is being proposed.
  if (state.pendingDecision !== present) {
    state.pendingDecision = present;
    state.pendingSinceTs = nowTs;
    state.glitchSinceTs = 0;
    state.pendingGlitches = 0;
    return;
  }

  state.glitchSinceTs = 0;
  let needSec = present ? DEBOUNCE_ON_SEC : DEBOUNCE_OFF_SEC;
  if (nowTs - state.pendingSinceTs >= needSec) {
    commitDecision(present);     // re-verified on this very poll
  }
}

// Too many interruptions inside one confirmation: the signal is unstable.
// Unstable is never proof of a healthy grid, only of an unhealthy one.
function onUnstable() {
  let candidate = state.pendingDecision;
  logEvent("ms_unstable", { candidate: candidate, glitches: state.pendingGlitches });
  if (candidate === false) {
    commitDecision(false);       // flapping during a loss still means: cut
  } else {
    clearCandidate();            // never turn ON off the back of a noisy signal
  }
}

// ===== MAIN LOOP =====

function tick() {
  let nowTs = nowSec();
  if (nowTs - state.lastConfigCheckTs >= CONFIG_CHECK_SEC) {
    state.lastConfigCheckTs = nowTs;
    checkInputConfig();
  }
  serviceQueue(nowTs);
  serviceWatchdogs(nowTs);
  evaluate(nowTs);
}

// ===== HTTP ENDPOINT =====

HTTPServer.registerEndpoint("ack", function (request, response) {
  if (request.method !== "POST") {
    response.code = 405; response.send(); return;
  }
  let ack;
  try { ack = JSON.parse(request.body); }
  catch (e) {
    response.code = 400;
    response.body = JSON.stringify({ error: "bad json" });
    response.send();
    return;
  }
  if (ack && ack.cmd_id && ack.ack === "EXECUTED") {
    logEvent("ms_executed", ack);
    clearPending(ack.cmd_id);
  }
  response.code = 200;
  response.body = JSON.stringify({ ok: true });
  response.headers = [["Content-Type", "application/json"]];
  response.send();
});

// ===== START =====

if (TARGETS.length > 3) {
  logEvent("ms_too_many_targets", { n: TARGETS.length, max: 3 });
}

state.lastConfigCheckTs = nowSec();
checkInputConfig();

state.voltageSource = detectVoltageSource();
logEvent("ms_voltage_source", {
  source: state.voltageSource === null ? "none" : state.voltageSource.type
});

state.pollTimer = Timer.set(POLL_MS, true, tick);
logEvent("ms_started", { schema: SCHEMA, n_targets: TARGETS.length });
