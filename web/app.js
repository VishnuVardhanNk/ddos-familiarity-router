const API = `http://${location.hostname}:3001`;
const OLLAMA = `http://${location.hostname}:11434`;
const TICK_MS = 3000;

const FAMILIES = {
  NetBIOS: { colour: '#d9796c', about: 'Attackers trick Windows name-service machines on the internet into sending their replies to your server.' },
  LDAP: { colour: '#dd9a62', about: 'Attackers abuse directory servers, which answer a small question with a very large reply aimed at your server.' },
  MSSQL: { colour: '#d8b866', about: 'Attackers abuse the discovery service of database servers so that its replies flood your server.' },
  UDP: { colour: '#a58fd0', about: 'A UDP flood throws huge numbers of junk packets at your server to use up its connection.' },
  UDPLag: { colour: '#8a97d6', about: 'A UDP-lag attack sends bursts of traffic timed to make a connection stutter rather than to take it down.' },
  Syn: { colour: '#cf7fb4', about: 'A SYN flood opens a huge number of connections and never finishes them, so the server runs out of room for real visitors.' },
  Portmap: { colour: '#6fb7c9', about: 'Attackers abuse the port-mapper service on other machines so that its replies flood your server.' }
};

const MEASURES = [
  ['flow_pkts_s', 'Packets per second'],
  ['flow_byts_s', 'Bytes per second'],
  ['pkt_len_mean', 'Average packet size (bytes)'],
  ['flow_duration', 'Connection length (microseconds)'],
  ['tot_fwd_pkts', 'Packets sent per connection'],
  ['tot_bwd_pkts', 'Packets sent back per connection'],
  ['syn_flag_cnt', 'SYN flags per connection'],
  ['ack_flag_cnt', 'ACK flags per connection'],
  ['tcp_share', 'Share using TCP']
];

const SHOWN = ['flow_pkts_s', 'flow_byts_s', 'pkt_len_mean', 'tot_bwd_pkts', 'tcp_share'];

const WORDS = {
  pkts: 'packets', pkt: 'packet', byts: 'bytes', len: 'length', fwd: 'forward', bwd: 'backward', tot: 'total',
  totlen: 'total length of', iat: 'gap between packets', cnt: 'count', std: 'spread', var: 'variance',
  s: 'per second', avg: 'average', act: 'active', psh: 'PSH', syn: 'SYN', ack: 'ACK', urg: 'URG', cwe: 'CWE'
};
const nice = feature => feature.split('_').map(word => WORDS[word] || word).join(' ');

const state = {
  batches: [], played: 0, lastId: -1, live: false, speed: 1,
  checked: 0, flagged: 0, incidents: [], picked: null,
  routes: { front: 0, copied: 0, forest: 0 }, labels: 0,
  normal: { flows: 0, sums: {} }, forestFeatures: []
};

const ROUTES = [
  ['front', 'Day-1 model (familiar traffic)'],
  ['copied', 'Copied from a labelled example'],
  ['forest', 'Random forest']
];

const address = new URLSearchParams(location.search);
const wanted = { attack: address.get('attack'), ask: address.get('ask') };
state.speed = Number(address.get('speed')) || 1;

const $ = id => document.getElementById(id);
const sleep = ms => new Promise(done => setTimeout(done, ms));
const number = n => Math.round(n).toLocaleString('en-US');
const compact = n => new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(n);
const clock = date => date.toTimeString().slice(0, 8);
const percent = (part, whole) => whole ? (100 * part / whole).toFixed(1) + '%' : '-';
const tidy = (key, value) => key === 'tcp_share' ? (100 * value).toFixed(0) + '%'
  : value >= 1000 ? compact(value) : value.toFixed(value < 10 ? 2 : 0);

async function get(path) {
  const response = await fetch(API + path);
  if (!response.ok) throw new Error(response.status);
  return response.json();
}

async function latestId() {
  const rows = await get('/live_batches?select=batch_id&order=batch_id.desc&limit=1');
  return rows.length ? rows[0].batch_id : -1;
}

async function load(from, to) {
  const range = `and=(batch_id.gte.${from},batch_id.lte.${to})`;
  const [batches, traffic, reasons, labels] = await Promise.all([
    get(`/live_batches?select=batch_id,flows,traffic_time&${range}&order=batch_id`),
    get(`/live_traffic?${range}`),
    get(`/live_reasons?${range}`).catch(() => []),
    get(`/live_routing?select=batch_id,labels_asked&model=eq.decision_tree&${range}`).catch(() => [])
  ]);
  const byId = new Map(batches.map(row =>
    [row.batch_id, { id: row.batch_id, flows: row.flows, time: new Date(row.traffic_time), families: {}, reasons: [], labels: 0 }]));
  for (const row of labels) if (byId.has(row.batch_id)) byId.get(row.batch_id).labels = row.labels_asked;
  for (const row of traffic) {
    const batch = byId.get(row.batch_id);
    if (batch) batch.families[row.label_family] = row;
  }
  for (const row of reasons) byId.get(row.batch_id)?.reasons.push(row);
  state.batches.push(...byId.values());
  state.lastId = to;
}

function addMeasures(target, row) {
  for (const [key] of MEASURES) target.sums[key] = (target.sums[key] || 0) + row[key] * row.flows;
}

function reset() {
  Object.assign(state, { played: 0, checked: 0, flagged: 0, incidents: [], picked: null, labels: 0,
    routes: { front: 0, copied: 0, forest: 0 }, normal: { flows: 0, sums: {} } });
}

function play(batch) {
  const index = state.played++;
  state.checked += batch.flows;
  state.labels = Math.max(state.labels, batch.labels);

  for (const [name, row] of Object.entries(batch.families)) {
    state.flagged += row.flagged;
    state.routes.front += row.flows - row.unfamiliar;
    state.routes.copied += row.from_memory;
    state.routes.forest += row.from_forest;
    if (!row.is_attack) {
      state.normal.flows += row.flows;
      addMeasures(state.normal, row);
      continue;
    }
    if (row.flows < 100) continue;
    let incident = state.incidents.findLast(i => i.family === name && i.last >= index - 5);
    if (!incident) {
      incident = { family: name, start: batch.time, first: index, flows: 0, flagged: 0, unfamiliar: 0, memory: 0, forest: 0,
        peak: 0, sums: {}, credit: {}, explained: {} };
      state.incidents.push(incident);
    }
    incident.end = batch.time;
    incident.last = index;
    incident.peak = Math.max(incident.peak, row.flows);
    const counted = new Set();
    for (const reason of batch.reasons.filter(r => r.label_family === name)) {
      const who = reason.decided_by;
      if (!counted.has(who)) {
        counted.add(who);
        incident.explained[who] = (incident.explained[who] || 0) + reason.connections_explained;
      }
      const credit = incident.credit[who] ??= {};
      credit[reason.feature] = (credit[reason.feature] || 0) + reason.contribution * reason.connections_explained;
    }
    incident.flows += row.flows;
    incident.flagged += row.flagged;
    incident.unfamiliar += row.unfamiliar;
    incident.memory += row.from_memory;
    incident.forest += row.from_forest;
    addMeasures(incident, row);
  }
}

function tickCounts(batch) {
  let normal = 0, flagged = 0, missed = 0;
  for (const row of Object.values(batch.families)) {
    if (!row.is_attack) normal += row.flows;
    else {
      flagged += Math.min(row.flagged, row.flows);
      missed += Math.max(0, row.flows - row.flagged);
    }
  }
  return { normal, flagged, missed };
}

function drawChart() {
  const canvas = $('chart');
  const box = canvas.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  canvas.width = box.width * ratio;
  canvas.height = box.height * ratio;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);

  const style = getComputedStyle(document.documentElement);
  const colours = ['--normal', '--attack', '--missed'].map(name => style.getPropertyValue(name).trim());
  const total = state.batches.length;
  const bins = Math.max(1, Math.min(total, Math.floor(box.width / 4)));
  const perBin = total / bins;
  const stacks = Array.from({ length: bins }, () => [0, 0, 0]);
  for (let i = 0; i < state.played; i++) {
    const counts = tickCounts(state.batches[i]);
    const stack = stacks[Math.min(bins - 1, Math.floor(i / perBin))];
    stack[0] += counts.normal;
    stack[1] += counts.flagged;
    stack[2] += counts.missed;
  }
  const tallest = Math.max(1, ...stacks.map(s => s[0] + s[1] + s[2]));
  const width = box.width / bins;

  ctx.strokeStyle = style.getPropertyValue('--line').trim();
  ctx.beginPath();
  ctx.moveTo(0, box.height - 0.5);
  ctx.lineTo(box.width, box.height - 0.5);
  ctx.stroke();

  stacks.forEach((stack, bin) => {
    let y = box.height - 1;
    stack.forEach((value, part) => {
      const height = value / tallest * (box.height - 6);
      ctx.fillStyle = colours[part];
      ctx.fillRect(bin * width + 0.5, y - height, Math.max(1, width - 1), height);
      y -= height;
    });
  });
}

function split(bar, key, routes) {
  const total = ROUTES.reduce((sum, [name]) => sum + routes[name], 0);
  bar.replaceChildren(...ROUTES.map(([name]) => {
    const part = document.createElement('span');
    part.style.width = (total ? 100 * routes[name] / total : 0) + '%';
    part.style.background = `var(--${name})`;
    return part;
  }));
  key.replaceChildren(...ROUTES.map(([name, label]) => {
    const item = document.createElement('li');
    const dot = document.createElement('i');
    dot.style.background = `var(--${name})`;
    const share = document.createElement('b');
    share.textContent = percent(routes[name], total);
    item.append(dot, label, share);
    return item;
  }));
}

function render() {
  split($('split'), $('splitKey'), state.routes);
  $('answeredHint').textContent = state.checked
    ? `Random forest needed for ${percent(state.routes.forest, state.checked)} · ${number(state.labels)} labels asked` : '';
  $('numChecked').textContent = number(state.checked);
  $('numFlagged').textContent = number(state.flagged);
  $('numIncidents').textContent = state.incidents.length;

  const batch = state.batches[state.played - 1];
  if (!batch) return;
  $('numClock').textContent = clock(batch.time);
  $('axisStart').textContent = clock(state.batches[0].time);
  $('axisEnd').textContent = clock(state.batches[state.batches.length - 1].time);

  const flagged = Object.values(batch.families).reduce((sum, row) => sum + row.flagged, 0);
  const underAttack = flagged > batch.flows * 0.25;
  $('status').className = 'status ' + (underAttack ? 'attack' : 'safe');
  $('statusTitle').textContent = underAttack ? 'Under attack' : 'Protected';
  $('statusDetail').textContent = underAttack
    ? `${number(flagged)} suspicious connections flagged in the last few seconds`
    : 'Traffic looks normal';

  if (state.picked === null && wanted.attack) {
    const index = state.incidents.findIndex(i => i.family === wanted.attack);
    if (index >= 0) state.picked = index;
  }
  drawChart();
  renderAttacks();
}

function renderAttacks() {
  const total = state.batches.length;
  const bands = $('bands');
  const list = $('incidents');
  bands.replaceChildren();
  list.replaceChildren();

  if (!state.incidents.length) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = 'No attacks seen yet.';
    list.append(empty);
  }

  state.incidents.forEach((incident, index) => {
    const colour = (FAMILIES[incident.family] || {}).colour || '#d9796c';
    const picked = state.picked === index;

    const band = document.createElement('div');
    band.className = 'band' + (picked ? ' picked' : '');
    band.style.left = 100 * incident.first / total + '%';
    band.style.width = 100 * (incident.last - incident.first + 1) / total + '%';
    band.style.background = colour;
    band.textContent = incident.family;
    band.dataset.index = index;
    bands.append(band);

    const card = document.createElement('div');
    card.className = 'incident' + (picked ? ' picked' : '');
    card.style.borderLeftColor = colour;
    card.dataset.index = index;
    const title = document.createElement('h4');
    title.textContent = `${incident.family} flood`;
    if (incident.last >= state.played - 3) {
      const tag = document.createElement('small');
      tag.textContent = 'happening now';
      title.append(tag);
    }
    const line = document.createElement('p');
    line.textContent = `${clock(incident.start)} to ${clock(incident.end)} · ${compact(incident.flows)} connections · ` +
      `${percent(incident.flagged, incident.flows)} flagged`;
    card.append(title, line);
    list.append(card);
  });

  renderEvidence();
  renderChips();
}

function reasons(incident) {
  const familiar = incident.flows - incident.unfamiliar;
  return [
    `${percent(incident.flagged, incident.flows)} of its ${number(incident.flows)} connections were flagged as attack.`,
    `${percent(familiar, incident.flows)} looked like traffic seen in training and was judged by the day-1 model.`,
    `${percent(incident.unfamiliar, incident.flows)} looked unfamiliar: ${percent(incident.memory, incident.flows)} was answered from ` +
      `labelled examples and ${percent(incident.forest, incident.flows)} by the random forest.`,
    (FAMILIES[incident.family] || {}).about || ''
  ].filter(Boolean);
}

function reasoning(incident) {
  const shares = {
    'decision tree': (incident.flows - incident.unfamiliar) / incident.flows,
    'random forest': incident.forest / incident.flows
  };
  return Object.entries(incident.explained)
    .filter(([who]) => shares[who] >= 0.01)
    .sort((a, b) => shares[b[0]] - shares[a[0]])
    .map(([who, explained]) => ({
      decided_by: who,
      share: (100 * shares[who]).toFixed(1) + '%',
      top: Object.entries(incident.credit[who])
        .map(([feature, sum]) => ({ measure: nice(feature), effect: sum / explained }))
        .sort((a, b) => Math.abs(b.effect) - Math.abs(a.effect)).slice(0, 5)
    }));
}

function answered(incident) {
  const parts = [
    ['the day-1 model, because the traffic looked familiar', incident.flows - incident.unfamiliar],
    ['copying the label of a similar connection already confirmed as attack', incident.memory],
    ['the random forest', incident.forest]
  ].sort((a, b) => b[1] - a[1]);
  return parts.filter(part => part[1] > 0).map(([how, flows]) => `${percent(flows, incident.flows)} by ${how}`);
}

function measures(group) {
  return Object.fromEntries(MEASURES.map(([key, label]) => [label, group.flows ? tidy(key, group.sums[key] / group.flows) : '-']));
}

function renderEvidence() {
  const panel = $('evidence');
  const incident = state.incidents[state.picked];
  panel.hidden = !incident;
  if (!incident) return;

  const heading = document.createElement('h4');
  heading.textContent = `Why the ${incident.family} flood was flagged`;
  const about = document.createElement('p');
  about.textContent = `${percent(incident.flagged, incident.flows)} of its ${number(incident.flows)} connections were flagged. ` +
    ((FAMILIES[incident.family] || {}).about || '');

  const answered = document.createElement('h5');
  answered.textContent = 'How this attack was answered';
  const bar = document.createElement('div');
  bar.className = 'split';
  const key = document.createElement('ul');
  key.className = 'splitKey';
  split(bar, key, { front: incident.flows - incident.unfamiliar, copied: incident.memory, forest: incident.forest });

  const table = document.createElement('table');
  const head = table.insertRow();
  for (const label of ['Measure (average)', 'This attack', 'Normal traffic']) {
    const cell = document.createElement('th');
    cell.textContent = label;
    head.append(cell);
  }
  const attack = measures(incident);
  const normal = measures(state.normal);
  for (const [, label] of MEASURES.filter(([name]) => SHOWN.includes(name))) {
    const row = table.insertRow();
    for (const value of [label, attack[label], normal[label]]) row.insertCell().textContent = value;
  }

  const why = document.createElement('table');
  const group = reasoning(incident)[0];
  if (group) {
    const whyHead = why.insertRow();
    for (const label of [`What pushed the ${group.decided_by} most`, 'Change in attack probability']) {
      const cell = document.createElement('th');
      cell.textContent = label;
      whyHead.append(cell);
    }
    for (const item of group.top.slice(0, 3)) {
      const row = why.insertRow();
      const effect = (item.effect >= 0 ? '+' : '') + item.effect.toFixed(3) + (item.effect >= 0 ? ' towards attack' : ' towards normal');
      for (const value of [item.measure, effect]) row.insertCell().textContent = value;
    }
  }
  why.hidden = !group;
  why.className = 'second';

  const action = document.createElement('div');
  action.className = 'ask';
  const button = document.createElement('button');
  button.textContent = 'Ask the assistant to explain';
  button.onclick = () => ask(`Why was the ${incident.family} flood classified as an attack?`);
  action.append(button);
  panel.replaceChildren(heading, about, answered, bar, key, why, table, action);
}

function renderChips() {
  const questions = ['What attacks happened?', 'Is my network safe right now?'];
  const incident = state.incidents[state.picked];
  if (incident) questions.push(`Why was the ${incident.family} flood classified as an attack?`);
  const chips = $('chips');
  if (chips.dataset.shown === questions.join('|')) return;
  chips.dataset.shown = questions.join('|');
  chips.replaceChildren(...questions.map(text => {
    const button = document.createElement('button');
    button.textContent = text;
    return button;
  }));
}

function pick(event) {
  const target = event.target.closest('[data-index]');
  if (!target) return;
  const index = Number(target.dataset.index);
  wanted.attack = null;
  state.picked = state.picked === index ? null : index;
  renderAttacks();
}

function setMode() {
  const waiting = !state.batches.length;
  $('mode').className = 'mode ' + (waiting ? '' : state.live ? 'live' : 'replay');
  $('mode').textContent = waiting ? 'Waiting' : state.live ? 'Live' : 'Replay of the last run';
  $('speeds').hidden = waiting || state.live;
  if (waiting) {
    $('statusTitle').textContent = 'Waiting for traffic';
    $('statusDetail').textContent = 'Start the stream with scripts/demo.sh';
  }
}

function goLive() {
  state.live = true;
  while (state.played < state.batches.length) play(state.batches[state.played]);
  setMode();
  render();
}

async function replay() {
  while (!state.live) {
    if (state.played < state.batches.length) {
      play(state.batches[state.played]);
      render();
    } else if (state.batches.length) {
      if (wanted.ask) ask(wanted.ask);
      wanted.ask = null;
      await sleep(8000);
      if (state.live) return;
      reset();
    }
    await sleep(TICK_MS / state.speed);
  }
}

async function poll() {
  try {
    const latest = await latestId();
    if (latest < state.lastId) return location.reload();
    if (latest > state.lastId) {
      await load(state.lastId + 1, latest);
      goLive();
    }
  } catch (error) {
    if (state.live) return location.reload();
  }
  setTimeout(poll, 1500);
}

const chat = { model: null, history: [] };

function say(role, text) {
  const bubble = document.createElement('div');
  bubble.className = 'message ' + role;
  bubble.textContent = text;
  $('messages').append(bubble);
  $('messages').scrollTop = $('messages').scrollHeight;
  return bubble;
}

function describe(incident) {
  return {
    type: incident.family + ' flood',
    what_it_is: (FAMILIES[incident.family] || {}).about,
    started: clock(incident.start),
    last_seen: clock(incident.end),
    still_happening: incident.last >= state.played - 3,
    lasted_seconds: Math.round((incident.end - incident.start) / 1000),
    connections: incident.flows,
    share_of_all_traffic_checked: percent(incident.flows, state.checked),
    busiest_moment_connections_per_tick: incident.peak,
    flagged_as_attack: percent(incident.flagged, incident.flows)
  };
}

function facts() {
  const picked = state.incidents[state.picked];
  return {
    status: $('statusTitle').textContent,
    traffic_time: $('numClock').textContent,
    connections_checked: state.checked,
    connections_flagged_as_attack: state.flagged,
    how_all_traffic_was_answered: {
      by_the_day_1_model_because_it_looked_familiar: percent(state.routes.front, state.checked),
      by_copying_the_label_of_a_similar_labelled_connection: percent(state.routes.copied, state.checked),
      by_the_random_forest: percent(state.routes.forest, state.checked),
      labels_asked_so_far: state.labels
    },
    attacks: state.incidents.map(describe),
    attack_the_user_selected: picked ? {
      ...describe(picked),
      how_it_was_answered: answered(picked),
      model_reasoning: reasoning(picked).map(group => ({
        decided_by: group.decided_by,
        share_of_this_attack_it_decided: group.share,
        measurements_that_moved_the_decision: group.top.map(item => ({
          measurement: item.measure,
          pushed: item.effect >= 0 ? 'towards attack' : 'towards normal',
          change_in_attack_probability: Number(item.effect.toFixed(3))
        }))
      })),
      model_reasoning_note: 'These come from following the model\'s own decision path on a sample of this attack\'s connections. ' +
        'Each number is how much that measurement moved the model\'s attack probability on average.',
      its_average_measurements: measures(picked),
      normal_traffic_average_measurements: measures(state.normal)
    } : null,
    how_detection_works: 'Models trained on an earlier day judge traffic that looks familiar. Traffic that looks unfamiliar copies the ' +
      'label of a similar connection whose true label was already requested, or is judged by a random forest if nothing similar is known.',
    measurements_the_random_forest_relies_on_most: state.forestFeatures
  };
}

function summary() {
  const lines = [`Status: ${$('statusTitle').textContent}. ${number(state.checked)} connections checked, ${number(state.flagged)} flagged as attack.`];
  lines.push(state.incidents.length
    ? 'Attacks: ' + state.incidents.map(i => `${i.family} flood from ${clock(i.start)} to ${clock(i.end)} (${percent(i.flagged, i.flows)} flagged)`).join('; ') + '.'
    : 'No attacks have been seen yet.');
  const picked = state.incidents[state.picked];
  if (picked) lines.push(`Selected: ${picked.family} flood. ` + reasons(picked).join(' '));
  return lines.join('\n');
}

async function findAssistant() {
  try {
    const tags = await (await fetch(OLLAMA + '/api/tags')).json();
    chat.model = tags.models.map(m => m.name).find(name => name.startsWith('gemma')) || null;
  } catch (error) {
    chat.model = null;
  }
  $('assistantState').className = 'assistantState ' + (chat.model ? 'on' : 'off');
  $('assistantState').textContent = chat.model ? chat.model + ', running on this computer' : 'offline';
  if (!chat.model) setTimeout(findAssistant, 15000);
}

async function ask(text) {
  if (!text.trim()) return;
  say('user', text);
  if (!chat.model) {
    say('note', 'The assistant is offline, so this is the automatic summary.');
    say('assistant', summary());
    return;
  }

  chat.history.push({ role: 'user', content: text });
  const bubble = say('assistant', '…');
  const system = 'You are the assistant inside a network protection app for a home or small business owner. ' +
    'Answer in plain, calm language in two to five short sentences, with no jargon and no markdown. ' +
    'Use only the facts below. If the answer is not in them, say you do not know. ' +
    'When asked why something was classified as an attack, answer in three steps. ' +
    'First give the first entry of how_it_was_answered with its percentage. ' +
    'Second name the two or three measurements that moved the model most. ' +
    'Third compare one or two of the attack\'s average measurements with normal traffic, using the numbers. ' +
    'When asked when or how much, give the times, duration and connection counts. ' +
    'When asked how the traffic was handled or how efficient the system is, use how_all_traffic_was_answered. ' +
    'Do not claim to know more about the detector than these facts state. The app detects attacks; it does not block them. ' +
    'Facts: ' + JSON.stringify(facts());

  try {
    const response = await fetch(OLLAMA + '/api/chat', {
      method: 'POST',
      body: JSON.stringify({ model: chat.model, stream: true, think: false, options: { temperature: 0.2 },
        messages: [{ role: 'system', content: system }, ...chat.history.slice(-6)] })
    });
    if (!response.ok) throw new Error(response.status);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let answer = '';
    let buffer = '';
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines.filter(l => l.trim())) {
        answer += JSON.parse(line).message?.content || '';
        bubble.textContent = answer || '…';
        $('messages').scrollTop = $('messages').scrollHeight;
      }
    }
    chat.history.push({ role: 'assistant', content: answer });
  } catch (error) {
    bubble.textContent = summary();
  }
}

async function start() {
  reset();
  findAssistant();
  say('assistant', 'Hello. I watch the traffic reaching your network. Ask me what happened, or select an attack and ask why it was flagged.');
  get('/feature_importance?select=feature&order=importance.desc&limit=8')
    .then(rows => { state.forestFeatures = rows.map(row => row.feature); }).catch(() => {});

  window.addEventListener('resize', drawChart);
  $('bands').addEventListener('click', pick);
  $('incidents').addEventListener('click', pick);
  $('chips').addEventListener('click', event => { if (event.target.tagName === 'BUTTON') ask(event.target.textContent); });
  $('ask').addEventListener('submit', event => {
    event.preventDefault();
    ask($('question').value);
    $('question').value = '';
  });
  $('speeds').addEventListener('click', event => {
    if (event.target.tagName !== 'BUTTON') return;
    state.speed = Number(event.target.dataset.speed);
    $('speeds').querySelectorAll('button').forEach(button => button.classList.toggle('on', button === event.target));
  });
  $('speeds').querySelectorAll('button').forEach(button => button.classList.toggle('on', Number(button.dataset.speed) === state.speed));
  renderChips();

  let first = -1;
  try {
    first = await latestId();
    if (first >= 0) await load(0, first);
  } catch (error) {
    first = -1;
  }
  await sleep(4000);
  const second = await latestId().catch(() => first);

  if (second > first) {
    await load(first + 1, second);
    goLive();
  } else {
    setMode();
    replay();
  }
  poll();
}

start();
