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
  if (!text) return null;

  // 운영 가이드 메시지인지 확인 (두 가지 포맷 모두 처리)
  const isOpsMsg = text.includes('출고 현황') || text.includes('출고 운영 가이드');
  if (!isOpsMsg) return null;

  // 시각 추출: "(Beta) 2026/04/18 22:00 기준" 또는 "\[(Beta) 2026/04/19 00:00 기준"
  const hourMatch = text.match(/\(?Beta\)?[^\n]*?(\d{2}):\d{2}\s*기준/);
  if (!hourMatch) {
    console.log('  시각 추출 실패 — 메시지 첫 줄:', text.split('\n')[0].slice(0, 80));
    return null;
  }
  const hour = parseInt(hourMatch[1], 10);

  // 도착보장 / 일반 수치 추출
  const daTot  = text.match(/도착보장[^\n]*총\s*(\d[\d,]+)건/)?.[1]?.replace(',', '');
  const daDone = text.match(/도착보장[^\n]*출고완료\s*(\d[\d,]+)건/)?.[1]?.replace(',', '');
  const ilTot  = text.match(/일반\s*:\s*총\s*(\d[\d,]+)건/)?.[1]?.replace(',', '');
  const ilDone = text.match(/일반\s*:[^\n]*출고완료\s*(\d[\d,]+)건/)?.[1]?.replace(',', '');

  if (!daTot || !ilTot) {
    console.log('  출고 현황 수치 추출 실패 — 도착보장:', daTot, '/ 일반:', ilTot);
    return null;
  }

  // 패킹장 추출 — 00:00 메시지는 "예측 실패" 포맷이라 없을 수 있음
  const packMatch = text.match(/(?:현재\s*)?운영\s*(?:중인\s*)?패킹장\s*:\s*(\d+)\s*대/);
  const pack = packMatch ? parseInt(packMatch[1], 10) : 0;

  console.log(`  추출: 도착보장총=${daTot} 완료=${daDone} / 일반총=${ilTot} 완료=${ilDone} / 패킹장=${pack}대`);

  return {
    hour,
    total: parseInt(daTot)      + parseInt(ilTot),
    done : parseInt(daDone || 0) + parseInt(ilDone || 0),
    pack,
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

  // Slack 최근 메시지 읽기 (여유있게 10개)
  const slackRes = await fetch(
    `https://slack.com/api/conversations.history?channel=${CHANNEL_ID}&limit=10`,
    { headers: { Authorization: `Bearer ${SLACK_TOKEN}` } }
  );
  const slackJson = await slackRes.json();

  if (!slackJson.ok) {
    console.error('Slack API 오류:', slackJson.error);
    process.exit(1);
  }

  console.log(`  슬랙 메시지 ${slackJson.messages?.length ?? 0}개 수신`);

  // 현재 KST 시간과 가장 가까운 정각 메시지를 찾기
  const kstHour = now.getUTCHours(); // kstNow()는 UTC+9 offset이 적용된 Date이므로 getUTCHours() = KST 시
  let parsed = null;
  for (const msg of slackJson.messages ?? []) {
    const txt = msg.text ?? '';
    const p = parseMsg(txt);
    if (!p) continue;
    // 현재 KST 시각 기준 최대 2시간 이내 메시지만 사용
    const diff = ((kstHour - p.hour) + 24) % 24;
    if (diff > 2) {
      console.log(`  ${p.hour}시 메시지는 현재 시각(${kstHour}시)과 차이가 크므로 스킵`);
      continue;
    }
    parsed = p;
    break;
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
