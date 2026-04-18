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
 */

const SLACK_TOKEN       = process.env.SLACK_TOKEN;
const FIREBASE_SECRET   = process.env.FIREBASE_DB_SECRET;
const FIREBASE_DB_URL   = process.env.FIREBASE_DB_URL   || 'https://ops-checklist-e9790-default-rtdb.firebaseio.com';
const CHANNEL_ID        = process.env.SLACK_CHANNEL_ID  || 'C06P23H3AJJ';

// ── KST 시간 유틸 ──────────────────────────────────────────────
function kstNow() {
  return new Date(Date.now() + 9 * 3_600_000);
}

// 운영일자: KST 오전 5시 이전이면 전날 날짜 반환
function opDate(kst) {
  const d = kst.getUTCHours() < 5
    ? new Date(kst.getTime() - 86_400_000)
    : kst;
  return d.toISOString().slice(0, 10);   // YYYY-MM-DD
}

// ── 메시지 파싱 ───────────────────────────────────────────────
function parseMsg(text) {
  if (!text || !text.includes('출고 현황')) return null;

  // 시각 추출: "(Beta) 2026/04/18 22:00 기준"
  const hourMatch = text.match(/\(Beta\)[^\n]*?(\d{2}):\d{2}\s*기준/);
  if (!hourMatch) return null;
  const hour = parseInt(hourMatch[1], 10);

  // 도착보장 / 일반 수치 추출
  const daTot  = text.match(/도착보장[^\n]*총\s*(\d+)건/)?.[1];
  const daDone = text.match(/도착보장[^\n]*출고완료\s*(\d+)건/)?.[1];
  const ilTot  = text.match(/일반\s*:\s*총\s*(\d+)건/)?.[1];
  const ilDone = text.match(/일반\s*:[^\n]*출고완료\s*(\d+)건/)?.[1];
  const pack   = text.match(/운영중인 패킹장\s*:\s*(\d+)대/)?.[1];

  if (!daTot || !ilTot) return null;

  return {
    hour,
    total : parseInt(daTot)  + parseInt(ilTot),
    done  : parseInt(daDone || 0) + parseInt(ilDone || 0),
    pack  : parseInt(pack   || 0),
  };
}

// ── Firebase REST 저장 ─────────────────────────────────────────
async function fbPut(path, data) {
  const url = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
  const res = await fetch(url, {
    method : 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body   : JSON.stringify(data),
  });
  if (!res.ok) {
    const msg = await res.text();
    throw new Error(`Firebase PUT 실패 [${res.status}]: ${msg}`);
  }
  return res.json();
}

// ── 메인 ──────────────────────────────────────────────────────
async function main() {
  console.log('▶ 출고 운영 지표 수집 시작');

  if (!SLACK_TOKEN || !FIREBASE_SECRET) {
    console.error('환경변수 SLACK_TOKEN 또는 FIREBASE_DB_SECRET 이 설정되지 않았습니다.');
    process.exit(1);
  }

  const now   = kstNow();
  const date  = opDate(now);
  const tsStr = now.toISOString().replace('T', ' ').slice(0, 16) + ' KST';
  console.log(`  실행 시각: ${tsStr}  /  운영일자: ${date}`);

  // Slack 최근 메시지 읽기
  const slackRes = await fetch(
    `https://slack.com/api/conversations.history?channel=${CHANNEL_ID}&limit=5`,
    { headers: { Authorization: `Bearer ${SLACK_TOKEN}` } }
  );
  const slackJson = await slackRes.json();

  if (!slackJson.ok) {
    console.error('Slack API 오류:', slackJson.error);
    process.exit(1);
  }

  // 첫 번째로 파싱 가능한 메시지 사용
  let parsed = null;
  for (const msg of slackJson.messages ?? []) {
    parsed = parseMsg(msg.text ?? '');
    if (parsed) break;
  }

  if (!parsed) {
    console.log('  파싱 가능한 운영 가이드 메시지 없음 — 스킵');
    process.exit(0);
  }

  const hourKey = String(parsed.hour).padStart(2, '0');
  console.log(`  파싱 완료: ${date}/${hourKey}시  total=${parsed.total}  done=${parsed.done}  pack=${parsed.pack}`);

  // Firebase 저장
  const path = `ops-trend/${date}/${hourKey}`;
  await fbPut(path, {
    total: parsed.total,
    done : parsed.done,
    pack : parsed.pack,
    ts   : tsStr,
  });

  console.log(`  Firebase 저장 완료: ${path}`);
  console.log('✅ 완료');
}

main().catch(e => {
  console.error('❌ 오류:', e.message);
  process.exit(1);
});
