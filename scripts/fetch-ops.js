/**
 * fetch-ops.js
 * 슬랙 #fc-gmp-operation-guide 채널에서 매시 최신 메시지를 읽어
 * Firebase Realtime Database의 ops-trend/{날짜}/{시간} 에 저장합니다.
 *
 * 필요한 환경변수 (GitHub Secrets):
 *   SLACK_TOKEN        - xoxp-... 로 시작하는 Slack User Token
 *   FIREBASE_DB_SECRET - Firebase 콘솔 > 프로젝트 설정 > 서비스 계정 > 데이터베이스 보안 비밀
 *   FIREBASE_DB_URL    - https://ops-checklist-e9790-default-rtdb.firebaseio.com
 *   SLACK_CHANNEL_ID   - C06P23H3AJJ
 *
 * Firebase 저장 구조: ops-trend/{YYYY-MM-DD}/{HH}
 *   total   : 도착보장총 + 일반총
 *   done    : 도착보장완료 + 일반완료
 *   waiting : 도착보장출고대기 + 일반출고대기  ← 잔여물량에 표시
 *   pack    : 현재 운영중인 패킹장 수 (예측실패 포맷에서는 0)
 *   cancel  : 도착보장취소 + 일반취소
 *   ts      : 저장 시점 epoch (밀리초) — 사이트에서 운영일 검증에 사용
 *   tsLabel : 사람이 읽는 시각 ("2026-04-24 11:35 KST")
 */

const SLACK_TOKEN     = process.env.SLACK_TOKEN;
const FIREBASE_SECRET = process.env.FIREBASE_DB_SECRET;
const FIREBASE_DB_URL = process.env.FIREBASE_DB_URL || 'https://ops-checklist-e9790-default-rtdb.firebaseio.com';
const CHANNEL_ID      = process.env.SLACK_CHANNEL_ID || 'C06P23H3AJJ';

function kstNow() {
  return new Date(Date.now() + 9 * 3_600_000);
}

// 운영일 계산: 오전 5시 미만이면 전날
function opDate(kst) {
  const d = kst.getUTCHours() < 5
    ? new Date(kst.getTime() - 86_400_000)
    : kst;
  return d.toISOString().slice(0, 10);
}

// 슬랙 메시지의 ts(초) → KST Date
function slackTsToKstDate(slackTs) {
  return new Date(parseFloat(slackTs) * 1000 + 9 * 3_600_000);
}

// ── 메시지 파싱 ───────────────────────────────────────────────
function parseMsg(text) {
  if (!text) return null;
  if (!text.includes('출고 현황') && !text.includes('출고 운영 가이드')) return null;

  const hourMatch = text.match(/\(?Beta\)?[^\n]*?(\d{2}):\d{2}\s*기준/);
  if (!hourMatch) {
    console.log('  [파싱실패] 시각 추출 불가:', text.split('\n')[0].slice(0, 60));
    return null;
  }
  const hour = parseInt(hourMatch[1], 10);

  const daLine = text.match(/도착보장[^\n]+/)?.[0] ?? '';
  const daTot  = daLine.match(/총\s*([\d,]+)건/)?.[1]?.replace(/,/g, '');
  const daWait = daLine.match(/출고대기\s*([\d,]+)건/)?.[1]?.replace(/,/g, '') ?? '0';
  const daDone   = daLine.match(/출고완료\s*([\d,]+)건/)?.[1]?.replace(/,/g, '') ?? '0';
  const daCancel = daLine.match(/취소\s*([\d,]+)건/)?.[1]?.replace(/,/g, '')   ?? '0';

  const ilLine   = text.match(/일반\s*:[^\n]+/)?.[0] ?? '';
  const ilTot    = ilLine.match(/총\s*([\d,]+)건/)?.[1]?.replace(/,/g, '');
  const ilWait   = ilLine.match(/출고대기\s*([\d,]+)건/)?.[1]?.replace(/,/g, '')  ?? '0';
  const ilDone   = ilLine.match(/출고완료\s*([\d,]+)건/)?.[1]?.replace(/,/g, '')  ?? '0';
  const ilCancel = ilLine.match(/취소\s*([\d,]+)건/)?.[1]?.replace(/,/g, '')     ?? '0';

  if (!daTot || !ilTot) {
    console.log('  [파싱실패] 총 건수 추출 불가 — 도착보장:', daTot, '/ 일반:', ilTot);
    return null;
  }

  const packMatch = text.match(/(?:현재\s*)?운영\s*(?:중인\s*)?패킹장\s*:\s*(\d+)\s*대/);
  const pack = packMatch ? parseInt(packMatch[1], 10) : 0;

  const result = {
    hour,
    total  : parseInt(daTot)    + parseInt(ilTot),
    done   : parseInt(daDone)   + parseInt(ilDone),
    waiting: parseInt(daWait)   + parseInt(ilWait),
    cancel : parseInt(daCancel) + parseInt(ilCancel),
    pack,
  };

  const isFail = text.includes('예측에 실패');
  console.log(`  [파싱성공] ${hour}시 | total=${result.total} done=${result.done} waiting=${result.waiting} cancel=${result.cancel} pack=${result.pack}${isFail ? ' (예측실패 포맷)' : ''}`);
  return result;
}

async function fbPut(path, data) {
  const url = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
  const res = await fetch(url, {
    method : 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body   : JSON.stringify(data),
  });
  if (!res.ok) throw new Error(`Firebase PUT 실패 [${res.status}]: ${await res.text()}`);
  return res.json();
}

async function main() {
  console.log('▶ 출고 운영 지표 수집 시작');

  if (!SLACK_TOKEN || !FIREBASE_SECRET) {
    console.error('환경변수 SLACK_TOKEN 또는 FIREBASE_DB_SECRET 누락');
    process.exit(1);
  }

  const now      = kstNow();
  const kstH     = now.getUTCHours();
  const date     = opDate(now);
  const tsLabel  = now.toISOString().replace('T', ' ').slice(0, 16) + ' KST';
  const tsEpoch  = Date.now();
  console.log(`  실행 시각: ${tsLabel} (KST ${kstH}시) / 운영일자: ${date}`);

  // 봇을 채널에 자동 입장
  const joinRes  = await fetch('https://slack.com/api/conversations.join', {
    method : 'POST',
    headers: { Authorization: `Bearer ${SLACK_TOKEN}`, 'Content-Type': 'application/json' },
    body   : JSON.stringify({ channel: CHANNEL_ID })
  });
  const joinJson = await joinRes.json();
  if (!joinJson.ok && joinJson.error !== 'already_in_channel') {
    console.warn('  채널 join 실패 (무시하고 계속):', joinJson.error);
  } else {
    console.log('  채널 입장 확인');
  }

  const slackRes  = await fetch(
    `https://slack.com/api/conversations.history?channel=${CHANNEL_ID}&limit=10`,
    { headers: { Authorization: `Bearer ${SLACK_TOKEN}` } }
  );
  const slackJson = await slackRes.json();

  if (!slackJson.ok) { console.error('Slack API 오류:', slackJson.error); process.exit(1); }
  console.log(`  슬랙 메시지 ${slackJson.messages?.length ?? 0}개 수신`);

  // ★ 핵심 수정: 메시지의 슬랙 ts(작성 시각)도 함께 검사
  // 1) 메시지가 작성된 운영일과 현재 운영일이 일치해야 함 (어제 메시지 차단)
  // 2) 그 중 현재 KST 시각과 가장 가까운 메시지 선택
  let parsed = null, bestDiff = 99, parsedSlackTs = null;
  for (const msg of slackJson.messages ?? []) {
    const p = parseMsg(msg.text ?? '');
    if (!p) continue;

    // 슬랙 메시지 작성 시점의 운영일 검증
    if (msg.ts) {
      const msgKst    = slackTsToKstDate(msg.ts);
      const msgOpDate = opDate(msgKst);
      if (msgOpDate !== date) {
        console.log(`  [스킵] ${p.hour}시 메시지 — 작성 운영일(${msgOpDate})이 오늘(${date})과 다름`);
        continue;
      }
    }

    const diff = ((kstH - p.hour) + 24) % 24;
    if (diff < bestDiff) { bestDiff = diff; parsed = p; parsedSlackTs = msg.ts; }
    if (diff === 0) break;
  }

  if (!parsed)         { console.log('  파싱 가능한 (오늘 운영일) 메시지 없음 — 스킵'); process.exit(0); }
  if (bestDiff > 3)    { console.log(`  최근 메시지가 현재 시각과 ${bestDiff}시간 이상 차이 — 스킵`); process.exit(0); }

  // ★ 추가 안전장치: 메시지 안의 hour 값도 운영일에 합당한지 검증
  // 운영일은 그 날짜 KST 05:00 ~ 익일 04:59. 시간(hour) 단독으론 검증 어려우니
  // 위에서 슬랙 ts로 이미 운영일 검증을 마쳤으므로 여기서는 추가 작업 없음

  const hourKey = String(parsed.hour).padStart(2, '0');
  await fbPut(`ops-trend/${date}/${hourKey}`, {
    total  : parsed.total,
    done   : parsed.done,
    waiting: parsed.waiting,
    cancel : parsed.cancel,
    pack   : parsed.pack,
    ts     : tsEpoch,   // ★ epoch 밀리초로 변경 (사이트 필터에서 사용)
    tsLabel: tsLabel,   // 사람이 읽기 위한 라벨 (디버깅용)
  });

  console.log(`  Firebase 저장 완료: ops-trend/${date}/${hourKey} (ts=${tsEpoch}, ${tsLabel})`);
  console.log('✅ 완료');
}

main().catch(e => { console.error('❌ 오류:', e.message); process.exit(1); });
