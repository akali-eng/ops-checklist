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
 *   ts      : 저장 시각 (KST)
 */

const SLACK_TOKEN     = process.env.SLACK_TOKEN;
const FIREBASE_SECRET = process.env.FIREBASE_DB_SECRET;
const FIREBASE_DB_URL = process.env.FIREBASE_DB_URL || 'https://ops-checklist-e9790-default-rtdb.firebaseio.com';
const CHANNEL_ID      = process.env.SLACK_CHANNEL_ID || 'C06P23H3AJJ';

function kstNow() {
  return new Date(Date.now() + 9 * 3_600_000);
}

function opDate(kst) {
  const d = kst.getUTCHours() < 5
    ? new Date(kst.getTime() - 86_400_000)
    : kst;
  return d.toISOString().slice(0, 10);
}

// ── 메시지 파싱 ───────────────────────────────────────────────
// 지원 포맷:
// [일반]  도착보장 : 총 N건 / 출고대기 N건 / 출고완료 N건 / 취소 N건
//         운영 가이드 섹션에 패킹장 정보 있음
// [예측실패] 출고대기가 0건, 패킹장 항목 없음, "예측에 실패하였습니다" 텍스트 포함
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
  const daDone = daLine.match(/출고완료\s*([\d,]+)건/)?.[1]?.replace(/,/g, '') ?? '0';

  const ilLine = text.match(/일반\s*:[^\n]+/)?.[0] ?? '';
  const ilTot  = ilLine.match(/총\s*([\d,]+)건/)?.[1]?.replace(/,/g, '');
  const ilWait = ilLine.match(/출고대기\s*([\d,]+)건/)?.[1]?.replace(/,/g, '') ?? '0';
  const ilDone = ilLine.match(/출고완료\s*([\d,]+)건/)?.[1]?.replace(/,/g, '') ?? '0';

  if (!daTot || !ilTot) {
    console.log('  [파싱실패] 총 건수 추출 불가 — 도착보장:', daTot, '/ 일반:', ilTot);
    return null;
  }

  const packMatch = text.match(/(?:현재\s*)?운영\s*(?:중인\s*)?패킹장\s*:\s*(\d+)\s*대/);
  const pack = packMatch ? parseInt(packMatch[1], 10) : 0;

  const result = {
    hour,
    total  : parseInt(daTot)  + parseInt(ilTot),
    done   : parseInt(daDone) + parseInt(ilDone),
    waiting: parseInt(daWait) + parseInt(ilWait),
    pack,
  };

  const isFail = text.includes('예측에 실패');
  console.log(`  [파싱성공] ${hour}시 | total=${result.total} done=${result.done} waiting=${result.waiting} pack=${result.pack}${isFail ? ' (예측실패 포맷)' : ''}`);
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

  const now   = kstNow();
  const kstH  = now.getUTCHours();
  const date  = opDate(now);
  const tsStr = now.toISOString().replace('T', ' ').slice(0, 16) + ' KST';
  console.log(`  실행 시각: ${tsStr} (KST ${kstH}시) / 운영일자: ${date}`);

  const slackRes  = await fetch(
    `https://slack.com/api/conversations.history?channel=${CHANNEL_ID}&limit=10`,
    { headers: { Authorization: `Bearer ${SLACK_TOKEN}` } }
  );
  const slackJson = await slackRes.json();

  if (!slackJson.ok) { console.error('Slack API 오류:', slackJson.error); process.exit(1); }
  console.log(`  슬랙 메시지 ${slackJson.messages?.length ?? 0}개 수신`);

  // 현재 KST 시각에 가장 가까운 메시지 선택 (최대 3시간 이내)
  let parsed = null, bestDiff = 99;
  for (const msg of slackJson.messages ?? []) {
    const p = parseMsg(msg.text ?? '');
    if (!p) continue;
    const diff = ((kstH - p.hour) + 24) % 24;
    if (diff < bestDiff) { bestDiff = diff; parsed = p; }
    if (diff === 0) break;
  }

  if (!parsed)         { console.log('  파싱 가능한 메시지 없음 — 스킵'); process.exit(0); }
  if (bestDiff > 3)    { console.log(`  최근 메시지가 현재 시각과 ${bestDiff}시간 이상 차이 — 스킵`); process.exit(0); }

  const hourKey = String(parsed.hour).padStart(2, '0');
  await fbPut(`ops-trend/${date}/${hourKey}`, {
    total  : parsed.total,
    done   : parsed.done,
    waiting: parsed.waiting,
    pack   : parsed.pack,
    ts     : tsStr,
  });

  console.log(`  Firebase 저장 완료: ops-trend/${date}/${hourKey}`);
  console.log('✅ 완료');
}

main().catch(e => { console.error('❌ 오류:', e.message); process.exit(1); });
