# pitchiq_pipeline.py
# ──────────────────────────────────────────────
# Run this on EC2: python3 pitchiq_pipeline.py
# Then open: http://YOUR_EC2_IP:5000
# ──────────────────────────────────────────────

import json
import time
import threading
import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from confluent_kafka import Producer
from flask import Flask, request, jsonify, render_template_string

# ── CONFIG ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = 'localhost:9092'
REPLAY_SPEED   = 0.05   # for old matches: 0.05 = very fast, 1.0 = real speed
POLL_INTERVAL  = 30     # for live matches: scrape every 30 seconds

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(levelname)s  %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)
pipeline_status = {'running': False, 'match_url': None, 'mode': None,
                   'events_sent': 0, 'error': None}

# ── SELENIUM DRIVER ───────────────────────────────────────────────────────────
def create_driver():
    """Create a headless Chrome browser."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )
    options.binary_location = '/usr/bin/chromium-browser'
    service = webdriver.chrome.service.Service('/usr/bin/chromedriver')
    return webdriver.Chrome(service=service, options=options)


# ── SCRAPER ───────────────────────────────────────────────────────────────────
def scrape_whoscored(url: str, driver) -> Optional[dict]:
    """Scrape match data from WhoScored. Returns matchdict or None."""
    try:
        driver.get(url)
        time.sleep(5)  # wait for JavaScript to load
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        element = soup.select_one('script:-soup-contains("matchCentreData")')
        if not element:
            log.error("Could not find matchCentreData on page.")
            return None
        raw = element.text.split("matchCentreData: ")[1].split(',\n')[0]
        return json.loads(raw)
    except Exception as e:
        log.error(f"Scraping error: {e}")
        return None


def is_live(matchdict: dict) -> bool:
    """Check if the match is currently live."""
    status = str(matchdict.get('status', '')).lower()
    # WhoScored uses 'live' or a period number for ongoing matches
    return 'live' in status or status in ['1', '2', 'ht']


def process_events(matchdict: dict) -> pd.DataFrame:
    """Clean and enrich events — same logic as your notebook."""
    # Build player and team lookup maps
    home_players = matchdict.get('home', {}).get('players', [])
    away_players = matchdict.get('away', {}).get('players', [])
    player_map = {p['playerId']: p['name']
                  for p in home_players + away_players}
    team_map = {
        matchdict['home']['teamId']: matchdict['home']['name'],
        matchdict['away']['teamId']: matchdict['away']['name'],
    }

    df = pd.DataFrame(matchdict.get('events', []))
    if df.empty:
        return df

    df.dropna(subset=['playerId'], inplace=True)
    df = df.where(pd.notnull(df), None)

    # Rename columns
    df = df.rename(columns={
        'eventId': 'event_id', 'expandedMinute': 'expanded_minute',
        'outcomeType': 'outcome_type', 'isTouch': 'is_touch',
        'playerId': 'player_id', 'teamId': 'team_id',
        'endX': 'end_x', 'endY': 'end_y',
        'blockedX': 'blocked_x', 'blockedY': 'blocked_y',
        'goalMouthZ': 'goal_mouth_z', 'goalMouthY': 'goal_mouth_y',
        'isShot': 'is_shot', 'cardType': 'card_type', 'isGoal': 'is_goal',
    })

    # Flatten nested dicts
    if 'period' in df.columns:
        df['period_display_name'] = df['period'].apply(
            lambda x: x['displayName'] if isinstance(x, dict) else str(x))
        df.drop(columns=['period'], inplace=True)
    if 'type' in df.columns:
        df['type_display_name'] = df['type'].apply(
            lambda x: x['displayName'] if isinstance(x, dict) else str(x))
        df.drop(columns=['type'], inplace=True)
    if 'outcome_type' in df.columns:
        df['outcome_type_display_name'] = df['outcome_type'].apply(
            lambda x: x['displayName'] if isinstance(x, dict) else str(x))
        df.drop(columns=['outcome_type'], inplace=True)

    # Ensure boolean columns exist
    for col in ['is_shot', 'is_goal', 'card_type', 'is_touch']:
        if col not in df.columns:
            df[col] = False
        df[col] = df[col].fillna(False).astype(bool)

    # Add player and team names
    df['player_name'] = df['player_id'].map(player_map).fillna('Unknown')
    df['team_name']   = df['team_id'].map(team_map).fillna('Unknown')

    # Ensure numeric columns
    for col in ['minute', 'second', 'x', 'y', 'end_x', 'end_y']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['minute'] = df['minute'].fillna(0).astype(int)
    df['second'] = df['second'].fillna(0)

    # Remove offside events (noise)
    if 'type_display_name' in df.columns:
        df = df[df['type_display_name'] != 'OffsideGiven']

    return df.reset_index(drop=True)


# ── KAFKA PRODUCER ────────────────────────────────────────────────────────────
def make_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'acks': 'all',
    })


def send_event(producer: Producer, event: dict, match_url: str):
    """Send one event to Kafka. Routes to specific topics too."""
    event['event_timestamp'] = datetime.utcnow().isoformat()
    event['match_url']       = match_url

    # Clean: convert NaN / numpy types to plain Python
    clean = {}
    for k, v in event.items():
        if isinstance(v, float) and np.isnan(v):
            clean[k] = None
        elif isinstance(v, (np.int64, np.int32)):
            clean[k] = int(v)
        elif isinstance(v, (np.float64, np.float32)):
            clean[k] = float(v)
        elif isinstance(v, (np.bool_,)):
            clean[k] = bool(v)
        else:
            clean[k] = v

    msg = json.dumps(clean)
    key = str(clean.get('id', ''))

    # Always send to main topic
    producer.produce('match.events', key=key, value=msg)

    # Also send to specific topic
    if clean.get('is_goal'):
        producer.produce('match.goals', key=key, value=msg)
    elif clean.get('is_shot'):
        producer.produce('match.shots', key=key, value=msg)
    elif clean.get('card_type'):
        producer.produce('match.cards', key=key, value=msg)

    producer.poll(0)  # trigger delivery callbacks
    pipeline_status['events_sent'] += 1


# ── OLD MATCH: REPLAY WITH TIMING ─────────────────────────────────────────────
def run_historical_replay(df: pd.DataFrame, producer: Producer,
                          match_url: str, speed: float):
    """
    Replay a finished match event by event.
    Events are sent in chronological order with simulated delays.
    """
    df = df.sort_values(['minute', 'second']).reset_index(drop=True)
    log.info(f"▶  Historical replay: {len(df)} events at {speed}x speed")

    prev_seconds = df.iloc[0]['minute'] * 60 + df.iloc[0]['second']

    for _, row in df.iterrows():
        curr_seconds = row['minute'] * 60 + row['second']
        delay = (curr_seconds - prev_seconds) / speed

        if delay > 0:
            time.sleep(min(delay, 5))  # cap at 5s so it never stalls

        send_event(producer, row.to_dict(), match_url)

        icon = ('⚽' if row.get('is_goal') else
                '🎯' if row.get('is_shot') else
                '🟨' if row.get('card_type') else '▶')
        log.info(f"{icon}  {row['minute']}'{int(row['second'])}\"  "
                 f"{row.get('team_name','?')}  {row.get('player_name','?')}  "
                 f"{row.get('type_display_name','')}")

        prev_seconds = curr_seconds

    producer.flush()
    log.info("🏁 Historical replay finished.")


# ── LIVE MATCH: POLL AND SEND NEW EVENTS ──────────────────────────────────────
def run_live_polling(url: str, driver, producer: Producer, match_url: str,
                     already_sent: set):
    """
    For a live match:
    1. Send all events already on the page immediately
    2. Then poll every 30s and send only NEW events as they appear
    """
    log.info("📡  Live match mode — polling every 30 seconds")

    while True:
        matchdict = scrape_whoscored(url, driver)
        if not matchdict:
            log.error("Lost connection to WhoScored — stopping.")
            break

        df = process_events(matchdict)
        new_rows = df[~df['id'].isin(already_sent)]

        for _, row in new_rows.iterrows():
            send_event(producer, row.to_dict(), match_url)
            already_sent.add(row['id'])
            log.info(f"📡 NEW EVENT  {row['minute']}'  "
                     f"{row.get('team_name','?')}  "
                     f"{row.get('type_display_name','')}")

        producer.flush()

        # If match has finished, stop polling
        if not is_live(matchdict):
            log.info("✅  Match finished. Stopping live polling.")
            break

        log.info(f"💤  Waiting {POLL_INTERVAL}s for next poll...")
        time.sleep(POLL_INTERVAL)


# ── MAIN PIPELINE ENTRY POINT ─────────────────────────────────────────────────
def run_pipeline(url: str, speed: float = REPLAY_SPEED):
    global pipeline_status
    pipeline_status.update({'running': True, 'match_url': url,
                             'events_sent': 0, 'error': None})

    producer = make_producer()
    driver   = create_driver()

    try:
        log.info(f"🔍  Scraping: {url}")
        matchdict = scrape_whoscored(url, driver)

        if not matchdict:
            pipeline_status['error'] = "Could not load match page."
            return

        df = process_events(matchdict)
        log.info(f"📊  Loaded {len(df)} events")

        if is_live(matchdict):
            # ── LIVE MATCH ──────────────────────────────────────────────
            pipeline_status['mode'] = 'live'
            # Send all events already visible on the page NOW
            sent_ids = set()
            for _, row in df.iterrows():
                send_event(producer, row.to_dict(), url)
                sent_ids.add(row['id'])
            producer.flush()
            # Then keep polling for new events
            run_live_polling(url, driver, producer, url, sent_ids)
        else:
            # ── OLD / FINISHED MATCH ────────────────────────────────────
            pipeline_status['mode'] = 'historical'
            run_historical_replay(df, producer, url, speed)

    except Exception as e:
        log.error(f"Pipeline error: {e}")
        pipeline_status['error'] = str(e)
    finally:
        driver.quit()
        pipeline_status['running'] = False
        log.info("🔒  Pipeline stopped.")


# ── FLASK CONTROL PANEL ───────────────────────────────────────────────────────
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
  <title>PitchIQ Control Panel</title>
  <style>
    body { font-family: Arial; background: #0f1117; color: #fff;
           display: flex; justify-content: center; padding: 60px; }
    .box { background: #1a1d27; border-radius: 16px; padding: 40px;
           width: 500px; box-shadow: 0 4px 30px #00000080; }
    h1   { color: #00d4ff; margin-bottom: 4px; }
    p    { color: #888; margin-bottom: 30px; }
    input, select { width: 100%; padding: 12px; margin: 8px 0 20px;
           background: #0f1117; border: 1px solid #333; color: #fff;
           border-radius: 8px; font-size: 14px; box-sizing: border-box; }
    button { width: 100%; padding: 14px; background: #00d4ff; color: #000;
             border: none; border-radius: 8px; font-size: 16px;
             font-weight: bold; cursor: pointer; }
    button:hover { background: #00b8d9; }
    .status { margin-top: 20px; padding: 16px; border-radius: 8px;
              background: #0f1117; border: 1px solid #333; font-size: 13px; }
    .live { color: #00ff88; } .error { color: #ff4444; }
    .info { color: #aaa; }
    a    { color: #00d4ff; }
  </style>
</head>
<body>
<div class="box">
  <h1>⚽ PitchIQ</h1>
  <p>Football Match Streaming Pipeline</p>

  <label>WhoScored Match URL</label>
  <input type="text" id="url"
    placeholder="https://www.whoscored.com/matches/.../live/..."
    value="{{ last_url }}">

  <label>Replay Speed (for old matches)</label>
  <select id="speed">
    <option value="0.05">Ultra Fast (show all events quickly)</option>
    <option value="0.2">Fast</option>
    <option value="1.0">Real Time (1x)</option>
  </select>

  <button onclick="startPipeline()">▶ Start Pipeline</button>
  <button onclick="stopCheck()"
    style="margin-top:10px; background:#333; color:#fff;">
    ⟳ Check Status
  </button>

  <div class="status" id="status">
    Status: <span class="{{ status_class }}">{{ status_text }}</span><br>
    {% if pipeline_status.mode %}
    Mode: <strong>{{ pipeline_status.mode }}</strong><br>
    {% endif %}
    Events sent: <strong>{{ pipeline_status.events_sent }}</strong><br>
    {% if pipeline_status.error %}
    Error: <span class="error">{{ pipeline_status.error }}</span><br>
    {% endif %}
    <br>
    <span class="info">
      📊 <a href="http://{{ grafana_ip }}:3000" target="_blank">
         Open Grafana Dashboard →</a>
    </span>
  </div>
</div>
<script>
function startPipeline() {
  const url   = document.getElementById('url').value;
  const speed = document.getElementById('speed').value;
  fetch('/start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({match_url: url, speed: parseFloat(speed)})
  }).then(r => r.json()).then(d => {
    document.getElementById('status').innerText = d.message;
    setTimeout(stopCheck, 3000);
  });
}
function stopCheck() {
  fetch('/status').then(r => r.json()).then(d => {
    document.getElementById('status').innerHTML =
      'Running: <b>' + d.running + '</b><br>' +
      'Mode: <b>' + (d.mode || '-') + '</b><br>' +
      'Events sent: <b>' + d.events_sent + '</b>' +
      (d.error ? '<br>Error: <span style="color:red">' + d.error + '</span>' : '');
  });
}
</script>
</body>
</html>
"""

@app.route('/')
def index():
    running = pipeline_status['running']
    return render_template_string(
        HTML_PAGE,
        pipeline_status = pipeline_status,
        last_url        = pipeline_status.get('match_url') or '',
        status_text     = 'Running ✅' if running else 'Idle',
        status_class    = 'live' if running else 'info',
        grafana_ip      = request.host.split(':')[0],
    )

@app.route('/start', methods=['POST'])
def start():
    if pipeline_status['running']:
        return jsonify({'message': '⚠️ Pipeline already running!'}), 400

    data      = request.get_json()
    match_url = data.get('match_url', '').strip()
    speed     = float(data.get('speed', REPLAY_SPEED))

    if 'whoscored.com' not in match_url:
        return jsonify({'message': '❌ Please enter a valid WhoScored URL'}), 400

    # Run in background thread so Flask doesn't block
    t = threading.Thread(target=run_pipeline, args=(match_url, speed), daemon=True)
    t.start()

    return jsonify({'message': f'✅ Pipeline started! Speed: {speed}x'})

@app.route('/status')
def status():
    return jsonify(pipeline_status)

@app.route('/stop', methods=['POST'])
def stop():
    # Simple flag — the pipeline checks this and stops
    pipeline_status['running'] = False
    return jsonify({'message': 'Stop signal sent.'})


# ── RUN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    log.info("🚀 PitchIQ Pipeline starting...")
    log.info(f"   Kafka broker : {KAFKA_BROKER}")
    log.info(f"   Replay speed : {REPLAY_SPEED}x")
    log.info("   Open browser : http://3.238.143.217:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)